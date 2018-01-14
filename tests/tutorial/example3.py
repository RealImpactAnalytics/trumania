from trumania.core import circus
import trumania.core.random_generators as gen
import trumania.core.operations as ops
import trumania.core.story as story
import trumania.components.time_patterns.profilers as profilers
import trumania.core.util_functions as util_functions

import pandas as pd

# each step?() function below implement one step of the third example of the
# tutorial documented at
# https://realimpactanalytics.atlassian.net/wiki/display/LM/Data+generator+tutorial


def build_circus():
    return circus.Circus(
        name="example3",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))


def add_music_repo(the_circus):

    repo = the_circus.create_population(
        name="music_repository", size=5,
        ids_gen=gen.SequencialGenerator(prefix="GENRE_"))

    repo.create_attribute(
        name="genre_name",
        init_values=["blues", "jazz", "electro", "pop", "rock"])

    repo.create_relationship(name="songs")


def add_songids_to_repos(the_circus):

    repo = the_circus.populations["music_repository"]

    song_id_gen = gen.SequencialGenerator(prefix="S_")
    added_songs = [song_id_gen.generate(size=1000) for _ in repo.ids]

    repo.get_relationship("songs").add_grouped_relations(

        # 5 genre ids
        from_ids=repo.ids,

        # 5 list of 1000 songs
        grouped_ids=added_songs)


def add_listener(the_circus):

    users = the_circus.create_population(
        name="user", size=1000,
        ids_gen=gen.SequencialGenerator(prefix="user_"))

    users.create_attribute(
        name="FIRST_NAME",
        init_gen=gen.FakerGenerator(method="first_name",
                                    seed=next(the_circus.seeder)))
    users.create_attribute(
        name="LAST_NAME",
        init_gen=gen.FakerGenerator(method="last_name",
                                    seed=next(the_circus.seeder)))


def add_listen_story(the_circus):

    users = the_circus.populations["user"]

    # using this timer means users only listen to songs during work hours
    timer_gen = profilers.WorkHoursTimerGenerator(
        clock=the_circus.clock, seed=next(the_circus.seeder))

    # this generate activity level distributed as a "truncated normal
    # distribution", i.e. very high and low activities are prevented.
    bounded_gaussian_activity_gen = gen.NumpyRandomGenerator(
        method="normal",
        seed=next(the_circus.seeder),
        loc=timer_gen.activity(n=20, per=pd.Timedelta("1 day")),
        scale=5
    ).map(ops.bound_value(lb=10, ub=30))

    listen = the_circus.create_story(
            name="listen_events",
            initiating_population=users,
            member_id_field="UID",

            timer_gen=timer_gen,
            activity_gen=bounded_gaussian_activity_gen
        )

    repo = the_circus.populations["music_repository"]

    listen.set_operations(

        users.ops.lookup(
            id_field="UID",
            select={
                "FIRST_NAME": "USER_FIRST_NAME",
                "LAST_NAME": "USER_LAST_NAME",
            }
        ),

        # picks a genre at random
        repo.ops.select_one(named_as="GENRE"),

        # picks a song at random for that genre
        repo.get_relationship("songs").ops.select_one(
            from_field="GENRE",
            named_as="SONG_ID"),

        ops.FieldLogger("events")
    )


def add_listen_and_share_stories(the_circus):
    """
    This is essentially a copy-paste of add_listen_story, + the update for the
    share story, in order to show the Chain re-usability clearly
    """

    users = the_circus.populations["user"]

    # using this timer means users only listen to songs during work hours
    timer_gen = profilers.WorkHoursTimerGenerator(
        clock=the_circus.clock, seed=next(the_circus.seeder))

    # this generate activity level distributed as a "truncated normal
    # distribution", i.e. very high and low activities are prevented.
    bounded_gaussian_activity_gen = gen.NumpyRandomGenerator(
        method="normal",
        seed=next(the_circus.seeder),
        loc=timer_gen.activity(n=20, per=pd.Timedelta("1 day")),
        scale=5
    ).map(ops.bound_value(lb=10, ub=30))

    listen = the_circus.create_story(
            name="listen_events",
            initiating_population=users,
            member_id_field="UID",

            timer_gen=timer_gen,
            activity_gen=bounded_gaussian_activity_gen
        )

    share = the_circus.create_story(
            name="share_events",
            initiating_population=users,
            member_id_field="UID",

            timer_gen=timer_gen,
            activity_gen=bounded_gaussian_activity_gen
        )

    repo = the_circus.populations["music_repository"]

    select_genre_and_song = ops.Chain(

        users.ops.lookup(
            id_field="UID",
            select={
                "FIRST_NAME": "USER_FIRST_NAME",
                "LAST_NAME": "USER_LAST_NAME",
            }
        ),

        # picks a genre at random
        repo.ops.select_one(named_as="GENRE"),

        # picks a song at random for that genre
        repo.get_relationship("songs").ops.select_one(
            from_field="GENRE",
            named_as="SONG_ID"),
    )

    listen.set_operations(
        select_genre_and_song,
        ops.FieldLogger("listen_events")
    )

    share.set_operations(
        select_genre_and_song,

        # picks a user this song is shared to
        users.ops.select_one(named_as="SHARED_TO_UID"),

        # note we could post-check when user shared a song to their own uid
        # here, in which case we can use DropRow to discard that share event

        ops.FieldLogger("share_events")
    )


def add_song_populations(the_circus):

    songs = the_circus.create_population(
        name="song", size=0,
        ids_gen=gen.SequencialGenerator(prefix="SONG_"))

    # since the size of the population is 0, we can create attribute without
    # providing any initialization
    songs.create_attribute(name="artist_name")
    songs.create_attribute(name="song_genre")
    songs.create_attribute(name="title")
    songs.create_attribute(name="duration_seconds")
    songs.create_attribute(name="recording_year")

    song_id_gen = gen.SequencialGenerator(prefix="S_")

    # generate artist names from a list of randomly generated ones, so we have
    # some redundancy in the generated dataset
    artist_name_gen = gen.NumpyRandomGenerator(
        method="choice",
        a=gen.FakerGenerator(
            method="name",
            seed=next(the_circus.seeder)).generate(size=200),
        seed=next(the_circus.seeder))

    title_gen = gen.FakerGenerator(method="sentence",
                                   seed=next(the_circus.seeder),
                                   nb_words=4,
                                   variable_nb_words=True)

    # generates recording years within a desired date range
    year_gen = gen.FakerGenerator(
            method="date_time_between_dates",
            seed=next(the_circus.seeder),
            datetime_start=pd.Timestamp("1910-10-20"),
            datetime_end=pd.Timestamp("2016-12-02")) \
        .map(f=lambda d: d.year)

    duration_gen = gen.ParetoGenerator(xmin=60,
                                       seed=next(the_circus.seeder),
                                       force_int=True,
                                       a=1.2)

    repo = the_circus.populations["music_repository"]
    repo_genre_rel = repo.get_attribute("genre_name")
    for genre_id, genre_name in repo_genre_rel.get_values().items():

        # an operation capable of creating songs of that genre
        init_attribute = ops.Chain(
            artist_name_gen.ops.generate(named_as="artist_name"),
            title_gen.ops.generate(named_as="title"),
            year_gen.ops.generate(named_as="recording_year"),
            duration_gen.ops.generate(named_as="duration_seconds"),
            gen.ConstantGenerator(value=genre_name).ops.generate(named_as="song_genre")
        )

        # dataframe of emtpy songs: just with one SONG_ID column for now
        song_ids = song_id_gen.generate(size=1000)
        emtpy_songs = story.Story.init_story_data(
            member_id_field_name="SONG_ID",
            active_ids=song_ids
        )

        # we can already adds the generated songs to the music repo relationship
        repo.get_relationship("songs").add_grouped_relations(
            from_ids=[genre_id],
            grouped_ids=[song_ids]
        )

        # here we generate all desired columns in the dataframe
        initialized_songs, _ = init_attribute(emtpy_songs)
        initialized_songs.drop(["SONG_ID"], axis=1, inplace=True)

        # this works because the columns of init_attribute match exactly the
        # ones of the attributes of the populations
        songs.update(initialized_songs)

    # makes sure year and duration are handled as integer
    songs.get_attribute("recording_year").transform_inplace(int)
    songs.get_attribute("duration_seconds").transform_inplace(int)


def add_listen_and_share_stories_with_details(the_circus):
    """
    This is again a copy-paste of add_listen_and_share_stories_with_details,
    (hopefully this helps to illustrate the progression), here showing the
    supplementary look-up on the attributes of the songs
    """

    users = the_circus.populations["user"]

    # using this timer means users only listen to songs during work hours
    timer_gen = profilers.WorkHoursTimerGenerator(
        clock=the_circus.clock, seed=next(the_circus.seeder))

    # this generate activity level distributed as a "truncated normal
    # distribution", i.e. very high and low activities are prevented.
    bounded_gaussian_activity_gen = gen.NumpyRandomGenerator(
        method="normal",
        seed=next(the_circus.seeder),
        loc=timer_gen.activity(n=20, per=pd.Timedelta("1 day")),
        scale=5
    ).map(ops.bound_value(lb=10, ub=30))

    listen = the_circus.create_story(
            name="listen_events",
            initiating_population=users,
            member_id_field="UID",

            timer_gen=timer_gen,
            activity_gen=bounded_gaussian_activity_gen
        )

    share = the_circus.create_story(
            name="share_events",
            initiating_population=users,
            member_id_field="UID",

            timer_gen=timer_gen,
            activity_gen=bounded_gaussian_activity_gen
        )

    repo = the_circus.populations["music_repository"]
    songs = the_circus.populations["song"]

    select_genre_and_song = ops.Chain(

        users.ops.lookup(
            id_field="UID",
            select={
                "FIRST_NAME": "USER_FIRST_NAME",
                "LAST_NAME": "USER_LAST_NAME",
            }
        ),

        # picks a genre at random
        repo.ops.select_one(named_as="GENRE"),

        # picks a song at random for that genre
        repo.get_relationship("songs").ops.select_one(
            from_field="GENRE",
            named_as="SONG_ID"),

        # now also reporting details of listened or shared songs
        songs.ops.lookup(
            id_field="SONG_ID",
            select={
                "artist_name": "SONG_ARTIST",
                "title": "SONG_TITLE",
                "recording_year": "SONG_YEAR",
                "duration_seconds": "SONG_DURATION",
            }
        ),
    )

    listen.set_operations(
        select_genre_and_song,
        ops.FieldLogger("listen_events")
    )

    share.set_operations(
        select_genre_and_song,

        # picks a user this song is shared to
        users.ops.select_one(named_as="SHARED_TO_UID"),

        # note we could post-check when user shared a song to their own uid
        # here, in which case we can use DropRow to discard that share event

        ops.FieldLogger("share_events")
    )


def run(the_circus):
    the_circus.run(
        duration=pd.Timedelta("5 days"),
        log_output_folder="output/example3",
        delete_existing_logs=True
    )


def step1():
    example3 = build_circus()
    add_music_repo(example3)
    add_songids_to_repos(example3)
    run(example3)


def step2():
    example3 = build_circus()
    add_music_repo(example3)
    add_songids_to_repos(example3)
    add_listener(example3)
    add_listen_story(example3)
    run(example3)


def step3():
    example3 = build_circus()
    add_music_repo(example3)
    add_songids_to_repos(example3)
    add_listener(example3)
    add_listen_and_share_stories(example3)
    run(example3)


def step4():
    example3 = build_circus()

    add_music_repo(example3)
    add_song_populations(example3)

    add_listener(example3)
    add_listen_and_share_stories_with_details(example3)
    run(example3)


if __name__ == "__main__":
    util_functions.setup_logging()
    step4()
