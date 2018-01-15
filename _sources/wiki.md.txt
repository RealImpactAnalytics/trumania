The purpose of this document is to provide a sufficient overview of each element of the data-generator in order to be able to write and execute data-generating circus 

# Contents

1. [Overview of circus elements and runtimes](#overview-of-circus)
    1. [Populations and attributes](#population-and-attributes)
    2. [Population relationships](#Population-relationships)
    3. [Data generators](#data-generators)
    4. [Stories, activity, timer and logs](#stories-activity-timer-logs)
    5. [Caveats](#caveats)
2. [Example 1: from hello world to basic CDR generator](#example1)
2. [Example 2: Point of sale simulation](#example2)
2. [Example 3: music streaming simulation](#example3)
2. [Example 4: reusability and circus persistence with DB](#example4)

## <a name="overview-of-circus"> </a> Overview of circus elements and runtimes

The data generator provides an API to design simulation circus and execute them in order to produce simulation logs. Those logs are the generated dataset. 

A data-generator circus is called a **circus.** In order to build a circus, we have to create a python programme that assemble instances of the following elements:

*   **populations**, which encapsulate acting entities of the circus (e.g. users, cars, shop, router...)
*   population **attributes**
*   population **relationship**, encapsulating one-to-one or one-to-many relationships between populations
*   **data generators**, either random or deterministic, they are the most atomic building block of generation. Once configured, they can generate an infinite stream of values, e.g. phone numbers, normally distributed numbers, coin flips...
*   **stories**, defined as a sequence of **operations**, they encapsulate some kind of behaviour. They are assembled from the elements above and produce logs when they are executed. 
*   a **clock**, which maintains the virtual time during the execution of a circus. Among other things the clock defines the simulation **time step**, which is the most granular increment of time that the circus will be able to manipulate.   
    to add the new item to

Each of these element is described in greater details below. 

  

There are two main execution times related to the data-generator:

*   **circus construction time**, which relates to the execution of the python code that you write to define populations and their relationships, configure data generators, define stories, configure the clock... The result of of this execution is a simulation circus, which can optionally be persisted, and is ready to be executed. 
*   **circus execution time**, which relates to whatever happens after you launch the run() operation on the circus above. In that phase, the stories you defined start being triggered along the timeline that you defined, producing logs and/or updating the state of the circus as they go. 

The data generator also comes with a crude persistence mechanism, simply referred to as the **DB**, that allows to save and load most circus elements above (keep in mind some persistence have not been implemented). This mechanism is optional: you can either write some construction-time code that build a circus and execute it directly, or you have the option to persist it in DB in between.Note that all random generators must be seeded and seeds are persisted in the DB, so the pseudo-generated datasets produced by the generator are repeatable. 

![](https://www.lucidchart.com/ac/confluence/placeholder.png?autoSize=1&macroId=c8c493e7-fb0b-497e-eb14-0b5a28c04c60&pageCount=1&instanceId=Confluence%3A6567865422&pages=&width=700&documentId=5fab3d9d-60c4-4e45-a91a-5979ed5cd133&align=left&type=rich&updated=1487229444761&height=500)




## <a name="populations-and-attributes"> </a>Populations and attributes

A **population** and its attribute(s) can be pictured as a dataframe where each row represent one **population member** of a certain type. For example, a circus with one million telecom subscribers could be modelled with one population called _subscriber_ having one million rows and some attributes like "name", "MSISDN", "city",... 

Each population member always gets automatically assigned a unique id. 

I can be misleading at first that, in the context of this generator, one population actually embodies many acting entities(population members). 



## <a name="populations-relationships"> </a>Population relationships

Population relationship can either encapsulate the link between the instances of one or two populations, or the link between the instances of a population(members) and some other entity, not modelled inside the circus. 

A relationship can be used to link population member of the same type, e.g. modelling a social network by linking human users together, or of different types, e.g. linking human users to their set of favourite location ids. 

A relationship can essentially be pictured as a join table, associating a "from id" to a "to id". The value of the "from id" is always the id of a population member whereas the "to id" is either also the id of a population member, or some other unrelated id. The later case is simply a shortcut that avoid to define populations without attributes. For example, if we need to model the relationship from some population to items like SIM card but do not need to keep any SIM card attributes, we could either create an sim_card population with no attribute and use that as the "to id" of the relationship, but we could also just put the id in the relationship, without creating the population. 



## <a name="data-generators"> </a>Data generators

### Generator

Generators are objects that expose a simple generate(size) method which, unsurprisingly, return a list of data point of the specified size. Here are a few of them:

*   ConstantGenerator: generates lists made of one single unique value, defined in the constructor, repeated many times
*   SequencialGenerator, generates lists of unique values using a simple incrementing counter, optionally prepended with a simple prefix
*   NumpyRandomGenerator: generates with a probability distribution family name and parameters, produces lists of random values distributed according that distribution. The generation is actually delegated to [numpy.random.RandomState](https://docs.scipy.org/doc/numpy/reference/generated/numpy.random.RandomState.html), so any family distribution supported by [numpy](http://www.numpy.org/) is available 
*   FakerGenerator: similarly to the previous one: this one generates list of values by delegating to some method the [Faker](https://pypi.python.org/pypi/Faker) library, so here again, any [feature available in Faker](https://faker.readthedocs.io/en/latest/providers.html) is available here



### DependentGenerator

All the generators presented above are execution time context agnostic in the sense that they do not need to be aware of any execution time information in order to produce data. There exist cases where generating data based on execution time data is handy. For example in a circus simulating purchases of items between sellers and buyers, one could want to generate a random flag that indicates the sellers becomes worried about low stock where the probability of such flag raise would get higher as the stock get lower. This could be modelled with a random generator of boolean (aka a Bernouilli distribution), whose parameters are computed at execution time based on stock level. We'll see an example of just that in example 2 below. 

There are just two DependentGenrator at the moment:

*   DependentTriggerGenerator: generates list of booleans corresponding to the example described above, by first mapping an execution time data into trigger probability 
*   DependentBulkGenerator: generates lists of "bulks", i.e. list of list of values. The values themselves are generated by any of the generator above whereas the size of each "bulk" is received at execution time. 



## <a name="stories-activity-timer-logs"> </a>Stories, activity, timer and logs

**Stories** are included in a circus in order to encapsulate dynamic behaviour, typically for generating logs and/or to update the state of the circus. An story is simply made of a linear sequence of steps, called **operations**, that define what to do. For example, an story to generate a dataset of purchase logs could be made of the following operations: randomly choose a shop, randomly choose an item, lookup its price, update the monetary accounts of both buyers and sellers, and finally output a logs recording the transaction. Story could probably benefit from supporting DAGs of operations in the future, as opposed to simple sequences at the moment. 



### Details of story

When building an story, one has to specify the following elements: 

*   the initiating population (the buyer, in the above example)
*   activity levels, which defines how frequently an story is executed on average. There is one activity level per population member, so if the story is initiated by a population with 10000 population members, we need to specify 10000 activity levels, which is why the activity levels are specified with a generator.
*   a timer profile: a probability distribution over an arbitrary time period specifying when executions are more likely to occur over that period. Such histogram could for example span a 24h period, thus specifying which moments of the day have higher activity, or a 7 days period, allowing to have different profiles during week days and weekend, or whatever. Such timer is handy to capture the fact that purchase event can only happen during office opening hours, or phone calls are more likely in the evenings except on weekends, ... 
*   a sequence of operations 

In the example timer profile below, note that you can specify some periods of the day when the action is impossible, simply by setting the probability to zero: 

![](https://www.lucidchart.com/ac/confluence/placeholder.png?autoSize=1&macroId=f229abd0-7e98-42d7-d668-e6df684c3343&pageCount=1&instanceId=Confluence%3A6567865422&pages=&width=700&documentId=82364925-0f97-4d5c-ad49-01281543ae37&align=left&type=rich&updated=1487234798023&height=500)



### Story execution mechanism 

In order to avoid design mistakes and keep in mind the power and limitations of stories, it help to be aware of their high level execution mechanism: 

*   at each clock step (say, every hour of simulation time):
    *   determine which population members will execute the story: this is influenced both by the activity levels and the timer profile. This boil down to selecting some rows in the corresponding population dataframe. 
    *   for each population member, execute the sequence of operations:
        *   column-wise: all population members will first execute the first operation 1, then the second,... 
        *   concurrently and without coordination: the current operation will be applied for all population members in arbitrary order within one clock step, which means that most state updating operations are subject to **race conditions**. An important exception to this is the one\_to\_one parameter of the relationship.select() operations, which prevents the same "to" side to be returned several times concurrently (e.g. this prevents a shop to sell several times the same item to different customers during one timestep)  
              
            

This design allows to "vectorize" the execution in the current implementation (and potentially to parallelise it in a later one), at the price of some inconsistencies. This is not as bad as it might sound, for the following reasons: 

*   in a lot of situations, only a small subset of population members are involved during any specific clock-step and thus collisions are typically rare.
*   The intend of the simulator is to generate test data and we usually do not care about generating a perfect dataset, but rather to obtain a dataset that has some chosen properties that are similar to a real one: so if monetary account of simulated users evolve according to transactions, with a erroneous glitch here and there, that's usually fine in the context of a test. 

If necessary, the following two mitigations approach can reduce the inconsistencies: 

*   reduce the size of the clock-step, thus removing the amount of concurrently active population members and thus the probability of collisions. This is likely to slow down the simulation though. 
*   use the one\_to\_one parameter of select_one, removing some collisions
*   Add some consistency checks in the story and apply a DropRow operation in case the row contains an inconsistency. Keep in mind that there is no such thing as an acid-rollback here, so any state mutation is not undone by the drop. 



### Operations

You can either code an operation from scratch to model to behaviour you need, or more likely just pick some of the ready-made operations part of the data-generator elements (see below). In both case an operation must simply respect the contract of:

*   transforming an **story_data**, which is a simply pandas dataframe whose columns are called **fields**, into an story\_data. There is one row in the story\_data per currently active population member during the current clock step. Typically an operation will add one or several fields and/ drop rows or just return it as-is
*   optionally, each operation can also emit logs, which will be automatically appended to the resulting CSV dataset by the generator. Typically only one operation outputs logs, the last one, although that is not an obligation. 

![](https://www.lucidchart.com/ac/confluence/placeholder.png?autoSize=1&macroId=718e0fce-723f-4d74-800e-527d3bd2c33a&pageCount=1&instanceId=Confluence%3A6567865422&pages=&width=700&documentId=ee3a3a0d-a8a5-4a9d-833f-851e02b783af&align=left&type=rich&updated=1485341361266&height=500)

  

Here is an overview of the available operations, some random, some deterministic, that can be picked from the elements of your circus: 

*   Population:
    *   lookup(): adds a fiels to the story_data by copying values from the population's attribute
    *   overwrite(): update the value of population attribues
    *   select\_one(): adds a field in the story\_data, filled with randomly-selected id of that population
    *   update(): add population members to that population 
*   Relationship
    *   select\_one(): adds a field to the story\_data with one randomly selected "to" side of the relationship of each population member row currently active
    *   select_many(): same as select_one(), but selects several "to" sides instead of one
    *   select_all()
    *   add(), remove(): update the content of that relationship
*   Clock
    *   timestamp(): adds a field to the story_data with a randomly generated timestamp within the current clock step 
*   Generator:
    *   generate(): adds a field to the story_data with randomly generated data 
*   Other stories (not attached to a circus element): 
    *   DropRows: remove rows from the story_data, based on some boolean condition 
    *   Apply: arbitrary computation based on one or several fields and appending one or several fields 
    *   Chain: this is just a composite of a sequence of stories. This is handy to be able to re-use part of the execution flow in several stories, as illustrated in example 3 below. 
    *   FieldLogger: does not update the story_data, but read fields from it and generate logs with their content 
*   Custom story: 
    *   You can also sub-class operations.Operation, operations.SideEffectOnly or operations.AddColumns to create your own operations

  
See the API for the 

![](https://www.lucidchart.com/ac/confluence/placeholder.png?autoSize=1&macroId=7f86ec24-e65a-4a0b-ee4a-99fce5b0347c&pageCount=1&instanceId=Confluence%3A6567865422&pages=&width=700&documentId=9744aa18-ad2d-4cd9-a77e-6ba9b6be5faa&align=left&type=rich&updated=1485345021421&height=500)

  




### More details on activity and timer profiles

This section provides more details on how the activity and timer profiles works behind the scene (this probably can safely be skipped during a first read). 

*   Even though activity levels are configured as frequencies, since we figured that would be easier to manipulate during configuration, what is actually used by the engine is the period => what we are actually configuring for each population member is an average duration between two executions. This duration is expressed internally in number of clock steps.
*   At execution time, the circus is maintaining a counter for each population member that specifies how many more clock steps each particular population member must wait until it triggers this specific story. This counter is called the **timer** of each population member for this story. At every clock step all the positive timers are decremented by one and all the population members whose timer is zero are included in the story execution of this clock step.
*   By default, stories are eternally repeating themselves, which is implemented internally by automatically reseting the timers to some positive number after an story execution (more on how this works below). 
*   You can de-activate the automatic timer reset during the configuration of an story. If you do that, the counter will reach -1 after an execution and remain to that value, which implies the populations will never be triggered by the clock (since it will never reach zero). You typically want to do that for stories that are executed as a reaction to others and not on a time basis as described here. In order to trigger the execution of an story as a reaction of another story, you can use the force\_act\_next() operation on the story, which essentially just sets the corresponding counters to zero.
*    Another operation you can do on timer is explicitly reseting the timers of some population members of some story. This is done simply by invoking the reset_timer() operation on that story. This is less often useful, thought could be desired if an story involved more than one population (e.g. a voice call) and we decide to model the fact that both A and B should have their timer reset after the story: the automatic timer reset described above would automatically reset the timer of the initiating population A, but you need to specifically invoke that operation for B. 

When the timers are reset, the following steps are performed by the engine:

*   it looks up the activity level for that population member and translate it into the corresponding number of average clock steps
*   it generates a random duration with this expected duration 
*   if the timer profile is flat, this duration is basically the new value of the timer. If not, we just distort the duration to land with higher probability in the moments that are more likely as defined by the timer profile. 

An example might help to clarify this: 

*   Assume we use the timer profile as defined by the squares below (more squares piled vertically means this time slot is more likely) and the user has an activity level of 4 times per day, i.e. an average period of  6 hours.
*   Also, assume the current time when we are resetting the timer is 8am, i.e. we are "after" the grey squares 
*   We generate a new random duration with mean 6h. Say we draw a 9, which corresponds to 9 squares on that histogram 
*   We simply "walk 9 squares along the histogram" and land on the 4pm time slot
*   Since 4pm is 8h later than 8am, we set the new timer counter to 8h, which is 4 clock steps (since the clock step in that example is 2h)

![](https://www.lucidchart.com/ac/confluence/placeholder.png?autoSize=1&macroId=a58d571e-8f1b-4de1-fff6-42d92aa4bf36&pageCount=1&instanceId=Confluence%3A6567865422&pages=&width=700&documentId=e5a5482c-09ac-4fc5-a2b7-43d011c61c08&align=left&type=rich&updated=1487234633579&height=500)



## <a name="caveats"> </a>Caveats

When using the data-generator, keep in mind the following current caveats: 

*   it is currently coded in python/pandas, and therefore runs on a single core in a single host. Since most of the logic is dataframe-oriented, re-coding everything (like 2000 lines of code) is Spark should be doable. 
*   it can be hard to debug: since the code you write is executed to assemble stories that are themselves executed, stacktraces can sometimes appear cryptic are may require knowledge of the internals to understand what is going on. Also, it _is_ possible to run an story inside a debugger and watch intermediary states as the operations are executed, but placing breakpoint at the appropriate place can appear tricky at first. 
*   it's (currently) not restartable: at the moment the DB persistence mechanism cannot be used after a circus has been started. This means you cannot generate one month of data, save the circus state, and come back later to add a couple of days. There exist a [ticket](https://realimpactanalytics.atlassian.net/browse/OASD-3001) for that feature. 
*   it's a young project, lot's of effort has been put abstract away the API from the implementation and execution details, although more clean-up could be added. Once we start writing a lot of circuses of course, this clean-up will be harder.   
      
    



<a name="example1"></a>Example 1: from hello world to basic CDR generator
==================================================

This example tries to walk you through most of the basic concepts of the data generator. An implementation of each step of this example is available on [github](https://github.com/RealImpactAnalytics/lab-data-generator/blob/develop/tests/tutorial/example1.py). 



Step 1 - Hello World
--------------------

*   create one Person population of size 1000 without any attributes
*   create one clock with step 1h
*   create one story where each Person emits a "hello world" log once per hour
*   run the simulation to generate 48 hours of logs



Step 2 - Add Person attributes and log timestamp 
-------------------------------------------------

*   add a first name attribute baked by a FakerGenerator
*   add a lookup operation to add the person first name as a field in the story_data of the story 
*   update the story so it contains a random timestamp 



Step 3 - Use a generator to generate durations at runtime
---------------------------------------------------------

So far the simulation is deterministic. Let's make it pseudo-random by generating some fields value at execution time.

*   Create a exponential generator with lambda = 1/60, we're going to use it to generate random durations, measured in seconds
*   Create a set of 1000 site ID and create a generator called locationGenerator that generate site id randomly picked from that set. 
*   Update the story with those generators  to add:
    *   a "talkingDuration" field
    *   a "location" field



Step 4 - Adding a relationship to capture mobility
--------------------------------------------------

Let's assume that each user is now only allowed to be in a limited set of locations, which could reflect knowledge of "frequent locations". A simple approach for that would be to store for each user their set of allows locations and then at execution time pick one randomly. This one-to-many relationship can be captured with a relationship. 

Let's first generate the set of allowed locations of each user. We'll assume for simplicity that each user has exactly 5 frequent locations. 

*   Add a relationship "allowed_locations" to the Person population
*   Initialize it with the Site ID generator so each person is related to exactly 5 sites (don't bother about making the sites unique per user, just call add_relationships 5 times)
*   Update the story so now the location is dynamically sample from the allowed\_locations of the Person, via a select\_one call



Step 5 - Make some locations more likely with a weighted relationship
---------------------------------------------------------------------

*   Update the User-> Location relationship above by adding a weight  so some location are more likely than others. For example 2 locations could both have a high 30% weight (which is reminiscent of "Home" and "work") and the other 3 could share the rest 



Step 6 - Add a relationship to capture a social network and use an execution runtime weight
-------------------------------------------------------------------------------------------

In the example above, the weight used in the select_one is carved into the relationship itself. In case the relationship is between populations, we can also use one property of the "to" population after of relationship. Let's illustrate that by updating the example so that a Person talks to another person of chosen within their social network, giving a higher preference to persons having a high popularity. 

*   Add a Popularity attribute to each person, initialised thanks to uniform distribution over \[0, 1\]
*   Add a second relationship to the Person population called "Friends". In this case, both "from" and "to" sides of the relationship are population members of a Person, though they could be of different populations as well). 
*   Use the make\_random\_bipartite_data method to initialise this relationship (do not set a weight)
*   Update the story so it now logs a "COUNTER\_PART\_NAME" field, which is the called person and is chosen at random among the active population's Friends, using a select_one.
*   Make sure the weight used in this select_one is now based on the Popularity attribute of the called person. 

Note that the above is a very simplistic way of generating a social network, more appropriate methods exist in case be need to keep some clustering or other aspects in considerations.



Step 7 - Add price and currency fields with a generic Apply operator
--------------------------------------------------------------------

Finally, as an illustration of ad-hoc computed field, now that our logs look more and more like CDR's, let's compute the billed amount of each log. 

*   write a python function that 
    *   takes as argument a dataframe with columns locationA, locationB and  duration
    *   returns a dataframe with 2 columns: "price" and "currency". This dataframe must have the same index as the input dataframe (since that's what is used behind the scene to inject the result into the story_data)
*   update the operation with an Apply operation using the method above



<a name="example2"></a>Example 2: Point of sale simulation
===================================

This example is inspired from the SND circus. We simulate here customers buying items from point of sales and illustrate slightly more advanced concepts like dependentGenerators, and triggers,.. Also, in this circus we'll have several stories executed by the circus.

An implementation of each step of this example is available on [github](https://github.com/RealImpactAnalytics/lab-data-generator/blob/develop/tests/tutorial/example2.py). 



Step 1- Point of sales population and stock level report 
----------------------------------------------------

Here we simply initialise the first population + some basic initial logs. 

*   Create PointOfSale population of size 100
*   confiture the clock with a step of 1h
*   Add the attributes administrator name, city, and company name (use a FakerGenerator for all that)

To keep things simple, we imagine each point of sale is selling just one kind of item. Each item sold and bought is explicitly modelled and identified by a unique id. We can model the point of sale stock of such items with a simple relationship:

*   Create a "items" relationship on the PointOfSale population. This relationship is currently empty. 

Finally, let's add a report story that will inform us on the stock levels at the end of each day: 

*   Create a new story "reportStockLevel" that triggers exactly once per day for each point of sale. This can be achieved with a ConstantDependentGenerator.
*   Add an operation to generate a timestamp field. Since we want the report at regular interval, set "random" to false so the timestamp is at exactly the same time every day. Also, you can change the log_format if required
*   Add an operation to add a "stock\_level" field: use the get\_neighbourhood_size operation of the relationship for that 
*   Add a Log operation to the story to actually produce logs. You can specify the fields and order if you want, or just use default. 

Execute the simulation and check everything looks ok: all reported stock sizes should be zero.



Step 2 - Initialise the stock of items 
---------------------------------------

*   Create a Sequencial ItemId generator that we are going to use to generate the ids of items
*   Attach it to the circus: we'll need this same generator later on for the restock story
*   Generate a list of 100 arrays of 5 items ids with this generator inside a simple for loop. 
*   Use the add_grouped operation on the "items" relationship of the PointOfSale so each population receive one list of 5 unique initial items

Execute the simulation and check everything looks ok: now all reported stock sizes should be always be 5. 



Step 3 - Add an initial dumb re-stock story with a DependentGenerator
----------------------------------------------------------------------

So far we've only seen stories whose primary purpose is to generate logs. Stories can also be used to update the state of the circus, as illustrated below.

At the moment all the point of sales's stock are eternally 5. Let's create a basic "restock" story to let the point of sale population regularly buy new items from their provider. This impacts the state of the circus since it will populate new members to the "items" relationship. In order to keep things super simple, we're not modelling the distributors here: any time a point of sale is restocking, it's actually just generating new items out of the blue and adding them to its stock (and then makes Mexico pay for it). 

This is not very realistic though: later on in this tutorial, we'll show how to trigger the restock story when the actual stock is getting low instead of on a timer-basis as described here. 

*   Create a "restock" story initiated by the PointOfSale population. Associate it with a default daily profile and an activity level of one trigger per week. You can use a ConstantGenerator for the activity, so that each POS has the same activity level, and use the activity() method of the timer to obtain the activity level corresponding to once per week. 
*   Add a lookup operation to include the administrator name in the story_data of the restock story 

In order to do a restock, we'll need 2 generators, one to generate the number of items added to the stock by the story, and another to generate the ids for each of those items. Let's start with the first generator: 

*   Create a stock size generator respecting the following empirical distribution (you can use a NumpyGenerator with choice method for that):
    *   5: 10%
    *   15: 20%
    *   20: 50%
    *   25: 20%
*   Add an operation to the restock story that is generating the field "restock_volume", using the generator above

So far so good. Creating a generator of item ids is easy, just re-use a sequencial generator we built before

*   retrieve the sequencial item id generator from the circus
In order to put them together, we first wrap the ItemId generator inside a DependentBulkGenerator, so that it's able to generator lists (bulks) of ids for each point of sale:

*   Wrap the ItemId inside a  DependentBulkGenerator so it now generates lists (i.e. bulks) of itemIds based on some input sizes that will be received at runtime (i.e. read from the current story_data dataframe of the story).

*   Add an operation to the "new\_items\_ids" field using this generator, specifying that the observed field is the restock_size field generated above.   
      
    

Note that the description above illustrate the nominal usage of a DependentGenerator, in which the generator is bound to the some observed field of the story_data that will only be available at simulation time. In this specific case, since that field is also coming from a generator (as opposed to an population attribute or anything else), a shortcut could have been to compose the 2 generators with the flatmap operator to obtain directly one generator of list of item ids. In the suggested solution of this exercise, this is illustrated in the step 3bis).

The last thing we need to do is to add those new item\_ids to the "items" relationship of the point of sale. Example 1 showed how to add\_relationships to a relationship statically, during circus construction time. We can also do that dynamically as part of an story, which we are going to do here. Since we are adding groups of items ids to each point of sale (as opposed to just one new relationship at a time), we do this with the add_grouped operation: 

*   add an operation to the restock story to add the items contained in the "new\_items\_ids" field to the stock of the corresponding point of sale. 
*   Add a Log operation to keep track of each re-stock, explicitly specify the list of logged fields and do not specify the "new\_items\_ids", so that the logs do not get cluttered by huge list of ids



Step 4 - Customer populations: modelling a purchase story with select_one(pop=True)
-------------------------------------------------------------------------------

Now that we have a story for the point of sale to actually get some stock, let's add one story for selling stuff to customers

*   Create a Customer population of size 2500, add them a first and last name 
*   Create a relationship called "my_items" on the Customer population. This relationship is current empty. 
*   Create a purchase story initiated by the customer, in which each customer tend to buy one item per day, exclusively during office hours:
    *   In order to force stories to happen only during office hours, set the timer to WorkHoursTimerGenerator: this is a predefined timer that sets the probability of a story to 0 for hours outside of office hours and to some constant during office hours.
    *   In order to have each customer to perform on average one story per day, simply fill in the activity levels with values corresponding to one occurence per day. The hard (and unnecessary) way to do it requires to inspect the timer: since the activity levels are specified in number of occurrences per duration of the timer period, so we can look at WorkHoursTimerGenerator and see it has a period of one week and thus set the activity levels to 7. The easier to the same thing is to use the activity() helper method of the timers, which provides the correct activity level corresponding to a desired frequency. 
    *   Once you know the activity level, you can plug it into the story thanks to a constant generator (which would imply that each customer would have the same activity level) or use any random generator whose expected value is set to this activity level (which would imply that some customer are more active than others, on average)  
          
        
*   Add a lookup operation to the purchase include the customer first and last name as fields in the story_data
*   Add an operation to this story to randomly select one point of sale and add it to the story\_data as "point\_of\_sale" field. Use the population.select\_one()  for that purpose
*   Add another lookup operation to include the point of sale city and company name as fields in the story_data
*   Add a FieldLogger operation to log all the fields mentioned above

Note that the logic above allows any customer to buy from any point of sale across the domain. If investigating geographic metrics were relevant to our use case, we could also explicitly model the movements of customers and select a point of sale according to the site where they are at the moment of the item purchase. 

Let's simulate the purchase transaction itself: 

*   Use the select_one operation of the "items" relationship of the point of sales to select the item that is sold to each customer. We need to specify a few parameters here: 
    *   one\_to\_one=True, meaning that even if 2 customers are buying the same shop, each one must be assigned a item from the relationship
    *   discard\_empty=True: if the shop is out of stock, the row should be evicted from the story\_data, essentially cancelling the buy story for that customer. Note that another approach here would be to retry to execute the story at the next clock step for the population member that failed to buy some item at that moment. You can achieve that by setting discard\_empty=False, in which case some rows might have empty values instead of purchased items in case the POS had nothing more to sell. You can then invoke explicitly a force\_act_next() of the same story for those populations, as described above, so the buyer tries again an maybe succeed, maybe because they pick a different POS or because that POS will have restocked some items.
    *   pop=True: when an item has been selected, it is automatically removed from the relationship of the point of sale
*   Use the "add" operation of the Customer "my_items" relationship to add the new item to their list of owned stuff
*   Add a timestamp, and log the transaction 

  

Note: there is probably a bug here: if 10 customers buy from a shop with stock level 5, I'm not sure what will happen... 



Step 5 - Refactor the restock story to trigger re-stock when stock is low with force\_act\_next()
--------------------------------------------------------------------------------------------------

As stated earlier, a caveat of the current simulation is that the re-stock stories are happening on a time-basis. In order to illustrate how to randomly or deterministically trigger an story from another story, let's refactor the circus so the customer purchase story now triggers the point of sale trigger story in case stock gets log. 

*   First remove the timer gen from the re_stock story => this imply the story is currently not happening anymore 
*   Add two  get\_neighbourhood\_size operations to the story: one before and one after updating the stock

The logic we'll put in place is that at the end of the purchase story, which is triggered by the customer population, we'll check the stock level of the point of sale population and, if "too low", we'll explicitly trigger the restock story of the point of sale. 

*   Add an the get\_neighbourhood\_size operation to the purchase story to retrieve the stock level of the point of sale and include it as "pos\_stock\_level" field 

We want to generate a "should\_restock" boolean field. One deterministic way to do this would be to write a predicate that returns true if "pos\_stock\_level" is below a certain threshold and wrap it inside an Apply operation, as already illustrated in example 1. In an attempt to illustrate something different, lets' use here a more probabilistic approach: let's convert the stock level into a probability to restock.  This is done below with a bounded\_sigmoid. We can then randomly generate boolean triggers based on those probability, which is done with a DependentTriggerGenerator. 

*   Use the bounded_sigmoid factory to create a decrementing S-shape function that is at its maximum when stock level is at 2 and is at 0 at 10. 
*   Use this sigmoid inside the DependentTriggerGenerator to include a "should\_restock" field in the purchase story story\_data

Now all we need is to force the execution of the restock_story when that boolean is true

*   use the force\_act\_next() operation on the restock\_story to triggered it conditioned by the story\_data field "should_restock". 



<a name="example3"></a>Example 3: music streaming simulation
=====================================

We'll build a simulation of music listening events. This will allow us to explore more advanced concepts like re-usable set of operation stories in a Chain and using stories during the circus initialisation. 



Step 1 - Create the music_repo population as a repository of song playlist 
----------------------------------------------------------------------

We're going to pretend that each song belongs to only one genre. Let's create a population whose sole purpose is to contain the list of songs of each genre:

*   Create a music_repository population with size = 5 : one for each genre
*   Add a "genre_name" attribute with the values \["blues", "jazz", "electro", "pop", "rock" \]
*   Add an empty "songs" relationship to this population
*   Create a songId sequencial generator, 
*   Loop over the 5 genre values and inside that loop, first generate 1000 song\_ids, with the generator above and add them with an add\_grouped() call to the "songs" relationship  



Step 2 - Listener population and music_listen event
----------------------------------------------

This is a simple population with one story to generate song_listen events

*   Create a user population of size 1000, with a first and last name
*   Create a listen_events story where
    *   users only listen to songs during work hours
    *   the number of songs a user will listen to per day should be a normal distribution around 20 per day where we cut out the extremes : nobody should listen to less than 10 songs per day, and nobody should listen to more than 30 songs per day.
*   Add an operation to add the user name and last name
*   Add an operation to that story to randomly select a genre among the genres of the music_repository population
*   add a select_one() operation to select the song that is listened to
*   Add a FieldLogger to produce the log of all that

Note that the timer model here is a bit naive in the sense that music listen events time pattern are likely to be more bursty in reality than what is described above, i.e. instead of spreading the listen_event of one user over the whole day, we should create burst of event. The "state" of the population allow to model that, although that is not explored here. 


Step 3 - Operation re-use with a Chain for a music_share event
--------------------------------------------------------------

*   Create another story call song_share event: modelling the fact that a song is shared by a user to another 
    
*   Add an operation to select randomly the user the song will be shared to. Ideally we'd model a social network here, though for the sake of simplicity, just pick a random user

At this point we'd need to select a genre and then a song of that genre. Instead of copy-pasting the logic of the music_listen event, let's make it a re-usable component instead: 

*   Move the 3 statements for the user name lookup + select genre + select one steps of the music event out of the story itself and inside a chain object 
*   Place this chain inside the music_listen event to keep the logic as before
*   Also place it in the music_share event


Step 4 - Initialising the song details with an story Chains at build time
--------------------------------------------------------------------------

So far we've seen stories that produce logs as well as story that update the circus (or do both) at simulation time, i.e. when the simulator is running. We can also re-use all the data-generating logic of the stories before starting the circus, during its initialisation. Let's illustrating that by replacing the mere song_id we're currently using with full song details.

*   First create a Songs population with size 0
*   create the "artist\_name", "song\_genre", "title", "recording_year" and "duration" attributes
*   Keep the "songs" relationship of the music_repo population, but remove its current initialisation 

We're simply going to fill-in everything we need inside the story_data of an story outside the circus, run it explicitly at initialisation time and update the "Songs" population

*   Create a python "genre" variable, set to "blues" for now 
*   Create an empty Chain object called init_attribute
*   Create an artist name generator and use it as first operation of the Chain, generating the "artist_name" field
*   Repeat the last step to generate the "title", "duration" and "publication_year" 
*   Use a ConstantGenerator to generate the "song_genre" field, using the value of the "genre" variable above

Since this Chain operation has been created outside the circus, as defined above it will never execute. A Chain (as any operation used in a story) is actually a python callable, so we can just invoke it on some initial story_data to generate songs. Let's do just that 5 times, once for each genre, and use the result to populate the content of the population: 

*   Create a for-loop that loop over each value of the "genre" attribute of the music repository population
*   Get rid of the python "genre" variable above and move the Chain declaration inside the genre for-loop. This now iteratively declares 5 Chain, each bound to one genre, each still doing nothing so far
*   Still inside the loop, using the Story.init\story\_data constructor, build an initial story_data containing one row for each of the songs generated
*   Simply invoke the chain with this story_data, the first return value should the the generated songs
*   Use the update() method of the Songs population to add all the rows to the population (maybe some column name/removal will be required here..) 
*   Also, populate the "songs" relationship for that genre in the music repository population. You can simply use add\_grouped\_relations for that. 

You can also update the listen and share stories so that now we report the details of the listened and shared songs, as opposed to just the id as before. 


<a name="example4"></a>Example 4: reusability and circus persistence with DB
=====================================================

The data generator has a very crude (CSV file based) but existing persistence functionality, which can be very handy in two situations:

1.  Meta-data reusability: some circus elements, e.g. complex random generator or population containing rich meta-data, could be re-used in several circus. We're going to illustrate this by saving the song repository in the "DB", completely un-related to the rest of that circus and showing how to load it inside the circus. It's worth noting that this mechanism also allows to load into a circus persisted data that has not been randomly generated. For example, we could imagine that we download a huge list of songs somewhere and feed them as a re-usable piece of data. Moreover, the same is true for probability distribution: it is easy to infer an empirical distribution from a client's data set, save it in the DB as a random generator and sample data from it at runtime, potentially adding some realism to a random simulation. 
2.  Circus creation involving populations of large size can potentially be very time consuming (hours is not unheard of). Having the possibility to save the whole circus state, together with all its populations, attributes, relationships and generators, allows to place an important milestone between the circus creation and circus execution. A simple extension of that mechanism would allow to stop and start the circus during its execution, although that is not currently implemented. 

This example is a continuation of example3. 


Step 1 - Move the song repository  the DB
-----------------------------------------

*   Move the logic for the song a well as music repository creation to separate methods
*   run those methods and use the DB API to save the 2 populations with ids "music\_repository" and "songs", within a namespace called "tutorial\_example4"
*   Update Circus initialisation of example 3 to load the "musi_repository" and "songs" populations from DB instead of creating them 


Step 2 - Save the circus before running it
------------------------------------------

*   At the end of the whole example 3 circus creation, use the DB API to save the whole circus to persistence. Note that stories cannot be saved in the DB, rather, they are saved "in code", err, like most dynamic behaviour I guess..
*   load the circus from persistence
*   attach the listen and share stories to it
*   execute  
      
