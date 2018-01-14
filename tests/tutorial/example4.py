from trumania.core import circus
import trumania.core.population as population
import trumania.core.random_generators as gen
import trumania.core.operations as ops
import trumania.core.story as story
import trumania.components.time_patterns.profilers as profilers
import trumania.core.util_functions as util_functions
import trumania.components.db as DB
import pandas as pd

# each step?() function below implement one step of the fourth example of the
# tutorial documented at
# https://realimpactanalytics.atlassian.net/wiki/display/LM/Data+generator+tutorial
# this is essentially a modification of example3, with some supplementary
# features demonstrating persistence


def build_music_repo():

    # this time we create a "detached" population, not connected to a circus
    repo = population.Population(
        circus=None,
        size=5,
        ids_gen=gen.SequencialGenerator(prefix="GENRE_"))

    repo.create_attribute(
        name="genre_name",
        init_values=["blues", "jazz", "electro", "pop", "rock"])

    repo.create_relationship(name="songs", seed=18)

    return repo


def add_song_to_repo(repo_population):

    songs = population.Population(
        circus=None,
        size=0,
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
            seed=1234).generate(size=200),
        seed=5678)

    title_gen = gen.FakerGenerator(method="sentence",
                                   seed=78961,
                                   nb_words=4,
                                   variable_nb_words=True)

    # generates recording years within a desired date range
    year_gen = gen.FakerGenerator(
            method="date_time_between_dates",
            seed=184,
            datetime_start=pd.Timestamp("1910-10-20"),
            datetime_end=pd.Timestamp("2016-12-02")) \
        .map(f=lambda d: d.year)

    duration_gen = gen.ParetoGenerator(xmin=60,
                                       seed=9874,
                                       force_int=True,
                                       a=1.2)

    repo_genre_rel = repo_population.get_attribute("genre_name")
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
        repo_population.get_relationship("songs").add_grouped_relations(
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

    return songs


def build_circus(name):
    return circus.Circus(
        name=name,
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))


def add_listener(the_circus):

    users = the_circus.create_population(
        name="user", size=5,
        ids_gen=gen.SequencialGenerator(prefix="user_"))

    users.create_attribute(
        name="FIRST_NAME",
        init_gen=gen.FakerGenerator(method="first_name",
                                    seed=next(the_circus.seeder)))
    users.create_attribute(
        name="LAST_NAME",
        init_gen=gen.FakerGenerator(method="last_name",
                                    seed=next(the_circus.seeder)))


def add_listen_and_share_stories_with_details(the_circus):

    users = the_circus.populations["user"]

    # using this timer means POS are more likely to trigger a re-stock during
    # day hours rather that at night.
    timer_gen = profilers.HighWeekDaysTimerGenerator(
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
    songs = the_circus.populations["songs"]

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


def step1():

    # this creates 2 populations: music_repo and songs
    music_repo = build_music_repo()
    songs = add_song_to_repo(music_repo)

    # saves them to persistence
    DB.remove_namespace(namespace="tutorial_example4")
    DB.save_population(music_repo, namespace="tutorial_example4",
                       population_id="music_repository")
    DB.save_population(songs, namespace="tutorial_example4",
                       population_id="songs")

    # build a new circus then loads and attach the persisted population to it
    example4_circus = build_circus(name="example4_circus")
    example4_circus.load_population(namespace="tutorial_example4",
                                    population_id="music_repository")
    example4_circus.load_population(namespace="tutorial_example4",
                                    population_id="songs")

    add_listener(example4_circus)


def step2():

    # this creates 2 populations: music_repo and songs
    music_repo = build_music_repo()
    songs = add_song_to_repo(music_repo)

    # saves them to persistence
    DB.remove_namespace(namespace="tutorial_example4")
    DB.save_population(music_repo, namespace="tutorial_example4",
                       population_id="music_repository")
    DB.save_population(songs, namespace="tutorial_example4",
                       population_id="songs")

    # build a new circus then loads and attach the persisted population to it
    example4_circus = build_circus(name="example4_circus")
    example4_circus.load_population(namespace="tutorial_example4",
                                    population_id="music_repository")
    example4_circus.load_population(namespace="tutorial_example4",
                                    population_id="songs")

    add_listener(example4_circus)

    # This saves the whole circus to persistence, with all its populations,
    # relationships, generators,...
    # This is independent from the 2 populations saved above: this time we no longer
    # have direct control on the namespace: the persistence mechanism use the
    # circus name as namespace
    example4_circus.save_to_db(overwrite=True)

    # example4bis should be an exact deep copy of example4_circus
    example4bis = circus.Circus.load_from_db(circus_name="example4_circus")

    # Stories are not serialized to CSV but rather serialized in code,
    # using humans as transducers
    add_listen_and_share_stories_with_details(example4bis)

    example4bis.run(
        duration=pd.Timedelta("5 days"),
        log_output_folder="output/example4",
        delete_existing_logs=True)


if __name__ == "__main__":
    util_functions.setup_logging()
    step2()
