# oracle-benchmark

L1-L2 Oracle Benchmark

# Empiric vs Chainlink: LUNA Crash

## Goal

Compare Empiric and Chainlink’s performance during a period of high stress: when Luna was crashing.

## Output 

Code that pulls this data from public blockchains in a verifiable way.
A jupyter notebook using the cleaned data as input and comparing the performances with Kaiko (or some other source) as the benchmark. 

## Background

Let’s use May 6 to May 25 as our time frame.

Empiric’s contract at the time was [https://goerli.voyager.online/contract/0x04a05a68317edb37d34d29f34193829d7363d51a37068f32b142c637e43b47a2#transactions?ps=10&p=9](https://goerli.voyager.online/contract/0x04a05a68317edb37d34d29f34193829d7363d51a37068f32b142c637e43b47a2#transactions?ps=10&p=9). This was before account contracts and before we used events so you’ll have to go through the transaction log to find the data. The data structure is : ticker, value, timestamp, source (eg if you look at data slots 2, 3, 4, 5 here [https://goerli.voyager.online/tx/0x63e66a4f98091eacfd47d35c4ad6aa1ae426d8a99d6d7f2ef3aeb6907de6ea5](https://goerli.voyager.online/tx/0x63e66a4f98091eacfd47d35c4ad6aa1ae426d8a99d6d7f2ef3aeb6907de6ea5))

Pull Chainlink data for the same time. They erased/cleared their Luna Ethereum contract but we can find it if you go to their docs [https://docs.chain.link/docs/data-feeds/price-feeds/addresses/](https://docs.chain.link/docs/data-feeds/price-feeds/addresses/) and put that in the way back machine for before mid-May. The next step will be to extract the data from that

----------------

# Installation & Requirements

- Using Python > 3.8

Install python dependencies

```
python -m venv benchmark-env
source benchmark-env/bin/activate
pip install --upgrade pip
CFLAGS=-I`brew --prefix gmp`/include LDFLAGS=-L`brew --prefix gmp`/lib pip install -r requirements.txt
```

Configurate CTC

```
ctc setup
   > put infura key
   > leave default until data dir
   > set data dir as ~/<PATHTOREPOSITORY>/oracle-benchmark/data/chainlink
```

## Results

Results are observable in `notebook.ipynb`

