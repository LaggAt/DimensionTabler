# DimensionTabler

Builds dimension tables with configurable/variable grainularity. Keeps those tables up-to-date. 
This makes reporting queries performant and doable. I use it with [grafana](https://grafana.com/).

Btw: this is a bad "denglish" name, suggest a better one.

## I'm interested, some details please

Let's do an example. We do gather tickers for a bunch of cryptocurrencies. 
We do that every minute for the top 10 currencies. This makes 864k lines per day.

Now we want to show them on a graph using grafana - great this works. 
later we join more and more information into this graph (e.g. balance we hold on each coin), the query gets slower and slower, and finally times out.

Here DimensionTabler jumps in. We want to be able to see long term ticker graphs, and we want to be able to see short term graphs in more detail. Let's put this in a table:

| Ticker date             | granularity        |       original count | dim.table count | reduced to % |
|-------------------------|--------------------|---------------------:|----------------:|-------------:|
| last 24 hours           | every ticker value | every minute = 1,440 |           1,440 |           0% |
| last 7 days             | 15 minutes         |    (6 days) = +8,640 |     /15' = +576 |          20% |
| last 30 days            | 1 hour             |              +33,120 |            +552 |         5.9% |
| last 90 days            | 4 hours            |              +86,400 |            +360 |         2.3% |
| before 90 days:         |                    |                      |                 |              |
| ... stat for 1 year     | 1 day              |             +396,000 |            +275 |         0.6% |
| ... for another 9 years | 1 day              |           +4,730,400 |          +3,285 |         0.1% |

So for 10 years our graph sql result counts 6,488 lines, the unfiltered result has 5,256,000 lines. 
Just by ignoring really unneded detail. DimensionTabler keeps this up-to-date and applies given rules.

## Usage

See [Example.py](https://github.com/LaggAt/DimensionTabler/blob/master/Example.py)

## Donate

Support me with bitcoins:

![Imgur](https://i.imgur.com/ltpF0A4m.png)

1MiToswzMsrhQEfmZbLQT8PHC68E5JhJzh

Thanks.
