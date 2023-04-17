# Collaboration and Topic Switches

Official code repositiory for the paper "Collaboration and Topic Switches in Science" by [Sara Venturini](https://saraventurini.github.io/)[^1], [Satyaki Sikdar](https://satyaki.net)[^1], [Francesco Rinaldi](https://sites.google.com/view/francescorinaldi/), [Francesco Tudisco](https://ftudisco.gitlab.io/post/), and [Santo Fortunato](https://www.santofortunato.net/). 

[^1]: SV and SS made equal contributions to this work.

## Data preprocessing
1. Create the `topic-switch` conda environment from `environment.yml` by running 
```
conda env create -f environment.yml
```

2. Create data directories by executing the following command:
```
mkdir -p data/Physics; mkdir -p data/CS; mkdir -p data/BioMed
```

3. Download the OpenAlex slices from [Zenodo](urlhere) inside `data/{FIELD}` directories. 
Eg: `Physics.zip` should be in `data/Physics`.

4. Extract the zipped slices, so you should have the following files inside `data/{FIELD}`: 
`works.parquet`, `works_authorships.parquet`, `works_concepts.parquet`, and `works_referenced_works.parquet`.   

## Running the eperiments 
* Run `notebooks/ExperimentI.pynb` or `notebooks/ExperimentII.pynb`
* More info coming soon..

## Analysis and generate plots 
* Run `notebooks/analysis.ipynb`
* More info coming soon..
