#### DISCLAIMER

This scenario works as-is, but some datasources and behaviors might
still be missing.

## Structure

This generator is divided in three steps:

1. `main_1_build.py` builds the circus: all the actors, relationships,
generators, etc and saves it to disk
2. `main_2_run.py` loads the circus from disk and runs it
3. `main_3_target.py` is an ad-hoc script to create targets based on
what the circus has produced

## Knobs to turn

Once you have coded your desired behavior in the circus, the complicated
part is setting the parameters correctly to get "realistic" values in
the end. The good news is that all the parameters are two places: you'll
find `static_params` in `main_1_build.py` and `runtime_params` in
`main_2_run.py`. The bad news is that it is not that easy to infer what
parameter will give what eventual output. You'll have to play with it a
bit.


### Actors

#### Products
* There is one actor per product type
* There is one actor instance per product id
* They have all their necessary attributes such tac_id for handsets

#### Sites
* There is one actor for sites
* There is one actor instance per site
* They have attributes to indicate geo information (e.g. geo_level1,
coordinates, population, urban/rural) and
which distributor is responsible to sell what product at that site
* They also have a Cell relationship to indicates which cells are under
each site
* All of the above is stored in the belgium geography
* They also have another relationship which captures which POS is at
which site
* The number of POS around a site is proportional to the site's
population

#### Distributors
* There are two distributor actors for the two levels of distributors
* Distributors L1 (level 1) buy from telcos and sell to Distributors L2
* Distributors L2 (level 2) buy from distributors L1 and sell to POS
* These actors are stored in the belgium geography
* They are organized in such a way that the hierarchy makes sense with
regards to provinces, regions, and the products they buy and sell from
one another -> This is why it is stored in the geography, it easier to
construct it by hand.
* They spend their time selling and restocking
* They have stock relationships for each product to keep track of the
product instance ids they own
* They have relationships to specify who is their provider for what
product, based on the geography

#### Customers
* There is one actor for customers
* There is one actor instance per customer
* Each customer has a set of sites it knows (the probability for a site
to be known by a customer is proportional to the site's population)
* Each clock tick, they can move from one site to another and decide to
buy from a POS at that site or not
* They have different probabilities for all their sites, to simulate
more frequent sites such as a home or work place

#### POS

* There is one actor for POS
* There is one actor instance per POS
* Each POS is assigned to a site, where the probability of a site to be
chosen is proportional to the site's population. The actual coordinates
of the POS are then randomly generated around the site's
* They have a relationship per product to indicate which
distributor L2 is their provider for the product
* They also have a stock relationship to store which POS had which
product instance id
* Each POS has an attractiveness attribute, this will indicate whether a
customer will chose them or not when buying a product. The final
attractiveness is based on two values: attractiveness base and
attractiveness delta. This allows us to simulate POS that are bettering
themselves at a certain rate instead of randomly going from one
attractiveness value to another.
* POS have an action that updates their attractiveness every day

#### Telcos

* There is one actor for Telcos
* There is one actor instance per Telco (usually one)
* Each Telco has one relationship per product to keep track of the
product instance id they have in stock
* Each Telco has one action per product to restock when needed

#### Field agents

* There is one actor for field agents
* There is one actor instance per field agent
* Each field agent has a set of sites it knows
* They have different probabilities for all their sites
* Each clock tick, they can move from one site to another and map a POS
that is present at their site

### Static parameters

The `static_params` in `main_1_build.py` contains the parameters needed
to build the circus. Here is a description of the different parameters:

* `circus_name`: The name of the circus to build
* `mean_known_sites_per_customer` : average number of sites that a
customers can move to.
* `clock_time_step`: How much time a clock step takes. If it is set to
1h, each clock tick will generate data for one hour in the simulated
world. See implications in the code documentation.
* `clock_start_date`: The time of the first clock tick.
* `geography`: The name of the geography to load in the _DB folder.
(Make sure you have downdloaded the `_DB` folder from S3. See README of
this repo)
* `n_telcos`: How many telcos to simulate (typically one)
* `n_pos`: How many pos to simulate
* `n_customers`: How many customers to simulate
* `n_field_agents`: How many field agents to simulate
* `products`: One entry per product (typically: sim, handset, mfs,
electronic_recharge, physical_recharge)
* `products.X.pos_bulk_purchase_sizes` and
`products.X.pos_bulk_purchase_sizes_dist`: These two are used to define
how many units of this product a POS buy will buy when it restocks. The
first one is a list of possible order size, and the second one is a list
of probabilities for each order size. For example: `[10, 50, 100]` and
`[0.5, 0.3, 0.2]` means that when a POS restocks, it has a 50% chance of
buying 10 units, a 30% chance of buying 50 units and a 20% chance of
buying 100 units.
* `products.X.pos_init_distro`: This is the path to an
optional distribution for how many POS should have how many units of
this product at the beginning of time. If this parameter is not
provided, the initial stock will be generated from the bulk purchase
generator defined by the two previous parameters.
* `products.X.telco_init_stock_customer_ratio`: what the initial stock
of a telco should be, based on the number of customers. If there are
10k customers and this ratio is set to .5, the telco will have 5k units
in stock.
* `products.X.product_types_num`: how many types of this product should
be generated
* `products.X.prefix`: the prefix to put in front of the product id

### Runtime parameters

The `runtime_params` in `main_2_run.py` contains the parameters needed
to run the circus. Here is a description of the different parameters:

* `circus_name`: The name of the circus to run
* `mean_daily_customer_mobility_activity` and
`std_daily_customer_mobility_activity`
* `mean_daily_fa_mobility_activity` and
`std_daily_fa_mobility_activity`
* `products`: One entry per product (typically: sim, handset, mfs,
electronic_recharge, physical_recharge)
* `products.X.customer_purchase_min_period_days` and
`products.X.customer_purchase_max_period_days`
* `products.X.max_pos_stock_triggering_pos_restock`
* `products.X.restock_sigmoid_shape`
* `products.X.pos_max_stock`
* `products.X.item_prices`
