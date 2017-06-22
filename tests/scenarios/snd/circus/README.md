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

### Static parameters

The `static_params` in `main_1_build.py` contains the parameters needed
to build the circus. Here is a description of the different parameters:

* `circus_name`: The name of the circus
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

