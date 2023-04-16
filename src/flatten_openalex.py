"""
Flatten Openalex JSON line files into individual CSVs and Parquets.
Original script at https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
"""
import csv
import glob
import gzip
import json
import os
import sys
from pathlib import Path
from typing import Union, Optional

import orjson  # faster JSON library
import pandas as pd
from tqdm.auto import tqdm

sys.path.extend(['../', './'])
from src.utils import convert_openalex_id_to_int, load_pickle, dump_pickle, reconstruct_abstract, read_manifest, \
    parallel_async

BASEDIR = Path('/N/project/openalex/ssikdar')  # directory where you have downloaded the OpenAlex snapshots
SNAPSHOT_DIR = BASEDIR / 'openalex-snapshot'
MONTH = 'feb-2023'
CSV_DIR = BASEDIR / 'processed-snapshots' / 'csv-files' / MONTH
PARQ_DIR = BASEDIR / 'processed-snapshots' / 'parquet-files' / MONTH
CSV_DIR.mkdir(parents=True, exist_ok=True)
PARQ_DIR.mkdir(parents=True, exist_ok=True)


csv_files = \
    {
        'institutions': {
            'institutions': {
                'name': os.path.join(CSV_DIR, 'institutions.csv.gz'),
                'columns': [
                    'institution_id', 'institution_name', 'ror', 'country_code', 'type', 'homepage_url',
                    'display_name_acroynyms', 'display_name_alternatives', 'works_count', 'cited_by_count',
                    'updated_date'
                ]
            },
            'ids': {
                'name': os.path.join(CSV_DIR, 'institutions_ids.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'openalex', 'ror', 'grid', 'wikipedia', 'wikidata', 'mag'
            ]
        },
        'geo': {
            'name': os.path.join(CSV_DIR, 'institutions_geo.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'city', 'geonames_city_id', 'region', 'country_code', 'country',
                'latitude',
                'longitude'
            ]
        },
        'associated_institutions': {
            'name': os.path.join(CSV_DIR, 'institutions_associated_institutions.csv.gz'),
            'columns': [
                'institution_id', 'associated_institution_id', 'relationship'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'institutions_counts_by_year.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'year', 'works_count', 'cited_by_count'
            ]
        }
    },

    'authors': {
        'authors': {
            'name': os.path.join(CSV_DIR, 'authors.csv.gz'),
            'columns': [
                'author_id', 'orcid', 'author_name', 'display_name_alternatives', 'works_count', 'cited_by_count',
                'last_known_institution', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'authors_ids.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'openalex', 'orcid', 'scopus', 'twitter', 'wikipedia', 'mag'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'authors_counts_by_year.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'year', 'works_count', 'cited_by_count'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'authors_concepts.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'works_count', 'cited_by_count', 'concept_id', 'concept_name', 'level',
                'score'
            ]
        },
        'hints': {
            'name': os.path.join(CSV_DIR, 'authors_hints.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'works_count', 'cited_by_count', 'most_cited_work'
            ]
        }
    },

    'concepts': {
        'concepts': {
            'name': os.path.join(CSV_DIR, 'concepts.csv.gz'),
            'columns': [
                'concept_id', 'concept_name', 'wikidata', 'level', 'description', 'works_count', 'cited_by_count',
                'updated_date'
            ]
        },
        'ancestors': {
            'name': os.path.join(CSV_DIR, 'concepts_ancestors.csv.gz'),
            'columns': ['concept_id', 'ancestor_id']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'concepts_counts_by_year.csv.gz'),
            'columns': ['concept_id', 'concept_name', 'year', 'works_count', 'cited_by_count']
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'concepts_ids.csv.gz'),
            'columns': ['concept_id', 'concept_name', 'openalex', 'wikidata', 'wikipedia', 'umls_aui', 'umls_cui',
                        'mag']
        },
        'related_concepts': {
            'name': os.path.join(CSV_DIR, 'concepts_related_concepts.csv.gz'),
            'columns': ['concept_id', 'related_concept_id', 'score']
        }
    },

    'venues': {
        'venues': {
            'name': os.path.join(CSV_DIR, 'venues.csv.gz'),
            'columns': [
                'venue_id', 'issn_l', 'issn', 'venue_name', 'type', 'publisher', 'works_count', 'cited_by_count',
                'is_oa',
                'is_in_doaj', 'homepage_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'venues_ids.csv.gz'),
            'columns': ['venue_id', 'venue_name', 'openalex', 'issn_l', 'issn', 'mag']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'venues_counts_by_year.csv.gz'),
            'columns': ['venue_id', 'venue_name', 'year', 'works_count', 'cited_by_count']
        },
    },

    'works': {
        'works': {
            'name': os.path.join(CSV_DIR, 'works.csv.gz'),
            'columns': [
                'work_id', 'doi', 'title', 'publication_year', 'publication_date', 'type', 'cited_by_count',
                'num_authors',
                'is_retracted', 'is_paratext', 'created_date', 'updated_date'
            ]
        },
        # Satyaki addition: put abstracts in a different CSV, save some space
        'abstracts': {
            'name': os.path.join(CSV_DIR, 'works_abstracts.csv.gz'),
            'columns': [
                'work_id', 'title', 'publication_year', 'abstract',
            ]
        },
        'host_venues': {
            'name': os.path.join(CSV_DIR, 'works_host_venues.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'venue_name', 'type', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'primary_location': {
            'name': os.path.join(CSV_DIR, 'works_primary_location.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'source_name', 'source_type', 'version', 'license', 'is_oa',
            ]
        },
        'locations': {
            'name': os.path.join(CSV_DIR, 'works_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'source_name', 'source_type', 'version', 'license', 'is_oa',
            ]
        },
        'alternate_host_venues': {
            'name': os.path.join(CSV_DIR, 'works_alternate_host_venues.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'venue_name', 'type', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'authorships': {
            'name': os.path.join(CSV_DIR, 'works_authorships.csv.gz'),
            'columns': [
                'work_id', 'author_position', 'author_id', 'author_name', 'institution_id',
                'institution_name', 'raw_affiliation_string', 'publication_year'
            ]
        },
        'biblio': {
            'name': os.path.join(CSV_DIR, 'works_biblio.csv.gz'),
            'columns': [
                'work_id', 'volume', 'issue', 'first_page', 'last_page'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'works_concepts.csv.gz'),
            'columns': [
                'work_id', 'publication_year', 'concept_id', 'concept_name', 'level', 'score'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'works_ids.csv.gz'),
            'columns': [
                'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
            ]
        },
        'mesh': {
            'name': os.path.join(CSV_DIR, 'works_mesh.csv.gz'),
            'columns': [
                'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui', 'qualifier_name', 'is_major_topic'
            ]
        },
        'open_access': {
            'name': os.path.join(CSV_DIR, 'works_open_access.csv.gz'),
            'columns': [
                'work_id', 'is_oa', 'oa_status', 'oa_url'
            ]
        },
        'referenced_works': {
            'name': os.path.join(CSV_DIR, 'works_referenced_works.csv.gz'),
            'columns': [
                'work_id', 'referenced_work_id'
            ]
        },
        'related_works': {
            'name': os.path.join(CSV_DIR, 'works_related_works.csv.gz'),
            'columns': [
                'work_id', 'related_work_id'
            ]
        },
    },
    }


def read_csvs(paths):
    """
    Return the concatenated df after reaching CSVs from paths
    """
    df = pd.concat([pd.read_csv(path) for path in paths], ignore_index=True)
    return df


def get_skip_ids(kind):
    """
    Get the set of IDs that have been merged with other IDs to skip over them
    """
    merged_entries_path = SNAPSHOT_DIR / 'data' / 'merged_ids' / kind
    if merged_entries_path.exists():
        merged_df = read_csvs(merged_entries_path.glob('*.csv.gz'))
        skip_ids = set(
            merged_df
            .id
        )
        skip_ids = {convert_openalex_id_to_int(id_) for id_ in skip_ids}
        print(f'{kind!r} {len(skip_ids):,} merged IDs')
    else:
        skip_ids = set()

    return skip_ids


def flatten_concepts():
    # read the merged entries and skip over those entries
    skip_ids = get_skip_ids('concepts')

    with gzip.open(csv_files['concepts']['concepts']['name'], 'wt', encoding='utf-8') as concepts_csv, \
            gzip.open(csv_files['concepts']['ancestors']['name'], 'wt', encoding='utf-8') as ancestors_csv, \
            gzip.open(csv_files['concepts']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv, \
            gzip.open(csv_files['concepts']['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(csv_files['concepts']['related_concepts']['name'], 'wt',
                      encoding='utf-8') as related_concepts_csv:

        concepts_writer = csv.DictWriter(
            concepts_csv, fieldnames=csv_files['concepts']['concepts']['columns'], extrasaction='ignore'
        )
        concepts_writer.writeheader()

        ancestors_writer = csv.DictWriter(ancestors_csv, fieldnames=csv_files['concepts']['ancestors']['columns'])
        ancestors_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv,
                                               fieldnames=csv_files['concepts']['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['concepts']['ids']['columns'])
        ids_writer.writeheader()

        related_concepts_writer = csv.DictWriter(related_concepts_csv,
                                                 fieldnames=csv_files['concepts']['related_concepts']['columns'])
        related_concepts_writer.writeheader()

        seen_concept_ids = set()

        files = list(glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'concepts', '*', '*.gz')))
        for jsonl_file_name in tqdm(files, desc='Flattening concepts...', unit=' file'):
            with gzip.open(jsonl_file_name, 'r') as concepts_jsonl:
                for concept_json in concepts_jsonl:
                    if not concept_json.strip():
                        continue

                    concept = json.loads(concept_json)

                    if not (concept_id := concept.get('id')) or concept_id in seen_concept_ids:
                        continue

                    concept_id = convert_openalex_id_to_int(concept_id)  # convert to int
                    if concept_id in skip_ids:  # skip over already merged IDs
                        continue

                    concept_name = concept['display_name']
                    seen_concept_ids.add(concept_id)

                    concept['concept_id'] = concept_id
                    concept['concept_name'] = concept_name
                    concepts_writer.writerow(concept)

                    if concept_ids := concept.get('ids'):
                        concept_ids['concept_id'] = concept_id
                        concept_ids['concept_name'] = concept_name
                        concept_ids['umls_aui'] = json.dumps(concept_ids.get('umls_aui'), ensure_ascii=False)
                        concept_ids['umls_cui'] = json.dumps(concept_ids.get('umls_cui'), ensure_ascii=False)
                        ids_writer.writerow(concept_ids)

                    if ancestors := concept.get('ancestors'):
                        for ancestor in ancestors:
                            if ancestor_id := ancestor.get('id'):
                                ancestors_writer.writerow({
                                    'concept_id': concept_id,
                                    'ancestor_id': ancestor_id
                                })

                    if counts_by_year := concept.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['concept_id'] = concept_id
                            count_by_year['concept_name'] = concept_name
                            counts_by_year_writer.writerow(count_by_year)

                    if related_concepts := concept.get('related_concepts'):
                        for related_concept in related_concepts:
                            if related_concept_id := related_concept.get('id'):
                                related_concepts_writer.writerow({
                                    'concept_id': concept_id,
                                    'related_concept_id': related_concept_id,
                                    'score': related_concept.get('score')
                                })
    return


def flatten_venues():
    skip_ids = get_skip_ids('venues')

    with gzip.open(csv_files['venues']['venues']['name'], 'wt', encoding='utf-8') as venues_csv, \
            gzip.open(csv_files['venues']['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(csv_files['venues']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        venues_writer = csv.DictWriter(
            venues_csv, fieldnames=csv_files['venues']['venues']['columns'], extrasaction='ignore'
        )
        venues_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['venues']['ids']['columns'], extrasaction='ignore')
        ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv,
                                               fieldnames=csv_files['venues']['counts_by_year']['columns'],
                                               extrasaction='ignore')
        counts_by_year_writer.writeheader()

        seen_venue_ids = set()

        files = list(glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'venues', '*', '*.gz')))
        for jsonl_file_name in tqdm(files, desc='Flattening venues...', unit=' file'):

            with gzip.open(jsonl_file_name, 'r') as venues_jsonl:
                for venue_json in venues_jsonl:
                    if not venue_json.strip():
                        continue

                    venue = orjson.loads(venue_json)

                    if not (venue_id := venue.get('id')) or venue_id in seen_venue_ids:
                        continue

                    venue_id = convert_openalex_id_to_int(venue_id)
                    if venue_id in skip_ids:  # skip over merged IDs
                        continue

                    venue_name = venue['display_name']
                    venue['venue_name'] = venue_name
                    seen_venue_ids.add(venue_id)

                    venue['issn'] = json.dumps(venue.get('issn'))
                    venues_writer.writerow(venue)

                    if venue_ids := venue.get('ids'):
                        venue_ids['venue_id'] = venue_id
                        venue_ids['venue_name'] = venue_name
                        venue_ids['issn'] = json.dumps(venue_ids.get('issn'))
                        ids_writer.writerow(venue_ids)

                    if counts_by_year := venue.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['venue_id'] = venue_id
                            count_by_year['venue_name'] = venue_name
                            counts_by_year_writer.writerow(count_by_year)

    return


def flatten_institutions():
    skip_ids = get_skip_ids('institutions')

    file_spec = csv_files['institutions']

    with gzip.open(file_spec['institutions']['name'], 'wt', encoding='utf-8') as institutions_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['geo']['name'], 'wt', encoding='utf-8') as geo_csv, \
            gzip.open(file_spec['associated_institutions']['name'], 'wt',
                      encoding='utf-8') as associated_institutions_csv, \
            gzip.open(file_spec['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        institutions_writer = csv.DictWriter(
            institutions_csv, fieldnames=file_spec['institutions']['columns'], extrasaction='ignore'
        )
        institutions_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=file_spec['ids']['columns'])
        ids_writer.writeheader()

        geo_writer = csv.DictWriter(geo_csv, fieldnames=file_spec['geo']['columns'])
        geo_writer.writeheader()

        associated_institutions_writer = csv.DictWriter(
            associated_institutions_csv, fieldnames=file_spec['associated_institutions']['columns']
        )
        associated_institutions_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=file_spec['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        seen_institution_ids = set()

        files = list(glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'institutions', '*', '*.gz')))
        for jsonl_file_name in tqdm(files, desc='Flattening Institutions...'):
            with gzip.open(jsonl_file_name, 'r') as institutions_jsonl:
                for institution_json in institutions_jsonl:
                    if not institution_json.strip():
                        continue

                    institution = orjson.loads(institution_json)

                    if not (institution_id := institution.get('id')) or institution_id in seen_institution_ids:
                        continue

                    institution_id = convert_openalex_id_to_int(institution_id)
                    if institution_id in skip_ids:
                        continue

                    institution_name = institution['display_name']
                    seen_institution_ids.add(institution_id)

                    # institutions
                    institution['institution_id'] = institution_id
                    institution['institution_name'] = institution_name
                    institution['display_name_acroynyms'] = json.dumps(institution.get('display_name_acroynyms'),
                                                                       ensure_ascii=False)
                    institution['display_name_alternatives'] = json.dumps(institution.get('display_name_alternatives'),
                                                                          ensure_ascii=False)
                    institutions_writer.writerow(institution)

                    # ids
                    if institution_ids := institution.get('ids'):
                        institution_ids['institution_id'] = institution_id
                        institution_ids['institution_name'] = institution_name
                        ids_writer.writerow(institution_ids)

                    # geo
                    if institution_geo := institution.get('geo'):
                        institution_geo['institution_id'] = institution_id
                        institution_geo['institution_name'] = institution_name
                        geo_writer.writerow(institution_geo)

                    # associated_institutions
                    if associated_institutions := institution.get(
                            'associated_institutions', institution.get('associated_insitutions')  # typo in api
                    ):
                        for associated_institution in associated_institutions:
                            if associated_institution_id := associated_institution.get('id'):
                                associated_institutions_writer.writerow({
                                    'institution_id': institution_id,
                                    'associated_institution_id': associated_institution_id,
                                    'relationship': associated_institution.get('relationship')
                                })

                    # counts_by_year
                    if counts_by_year := institution.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['institution_id'] = institution_id
                            count_by_year['institution_name'] = institution_name
                            counts_by_year_writer.writerow(count_by_year)
    return


def flatten_authors(files_to_process: Union[str, int] = 'all'):
    skip_ids = get_skip_ids('authors')

    file_spec = csv_files['authors']

    authors_csv_exists = Path(file_spec['authors']['name']).exists()
    ids_csv_exists = Path(file_spec['ids']['name']).exists()
    counts_by_year_csv_exists = Path(file_spec['counts_by_year']['name']).exists()
    authors_concepts_csv_exists = Path(file_spec['concepts']['name']).exists()
    authors_hints_csv_exists = Path(file_spec['hints']['name']).exists()

    with gzip.open(file_spec['authors']['name'], 'at', encoding='utf-8') as authors_csv, \
            gzip.open(file_spec['ids']['name'], 'at', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['counts_by_year']['name'], 'at', encoding='utf-8') as counts_by_year_csv, \
            gzip.open(file_spec['concepts']['name'], 'at', encoding='utf-8') as authors_concepts_csv, \
            gzip.open(file_spec['hints']['name'], 'at', encoding='utf-8') as authors_hints_csv:

        authors_writer = csv.DictWriter(
            authors_csv, fieldnames=file_spec['authors']['columns'], extrasaction='ignore'
        )
        if not authors_csv_exists:
            authors_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=file_spec['ids']['columns'])
        if not ids_csv_exists:
            ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=file_spec['counts_by_year']['columns'])
        if not counts_by_year_csv_exists:
            counts_by_year_writer.writeheader()

        authors_concepts_writer = csv.DictWriter(
            authors_concepts_csv, fieldnames=file_spec['concepts']['columns'], extrasaction='ignore'
        )
        if not authors_concepts_csv_exists:
            authors_concepts_writer.writeheader()

        authors_hints_writer = csv.DictWriter(
            authors_hints_csv, fieldnames=file_spec['hints']['columns'], extrasaction='ignore'
        )
        if not authors_hints_csv_exists:
            authors_hints_writer.writeheader()

        print(f'This might take a while, like 6-7 hours..')

        finished_files_pickle_path = CSV_DIR / 'temp' / 'finished_authors.pkl'
        finished_files_pickle_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

        if finished_files_pickle_path.exists():
            finished_files = load_pickle(finished_files_pickle_path)  # load the pickle
            print(f'{len(finished_files)} existing files found!')
        else:
            finished_files = set()

        authors_manifest = read_manifest(kind='authors', snapshot_dir=SNAPSHOT_DIR / 'data')
        files = [str(entry.filename) for entry in authors_manifest.entries]
        # files = map(str, glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')))
        files = [f for f in files if f not in finished_files]

        if files_to_process == 'all':
            files_to_process = len(files)

        print(f'{files_to_process=}')

        for i, jsonl_file_name in tqdm(enumerate(files), desc='Flattening authors...', unit=' file',
                                       total=files_to_process):
            if i > files_to_process:
                break

            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                authors_jsonls = authors_jsonl.readlines()

            authors_rows, ids_rows, counts_by_year_rows, authors_concepts_rows, author_hints_rows = [], [], [], [], []

            for author_json in tqdm(authors_jsonls, desc='Parsing JSONs', leave=False, unit=' line', unit_scale=True):
                if not author_json.strip():
                    continue

                author = orjson.loads(author_json)

                if not (author_id := author.get('id')):
                    continue
                author_id = convert_openalex_id_to_int(author_id)
                if author_id in skip_ids:
                    continue

                author_name = author['display_name']

                # authors
                author['author_id'] = author_id
                author['author_name'] = author_name
                author['display_name_alternatives'] = json.dumps(author.get('display_name_alternatives'),
                                                                 ensure_ascii=False)
                last_known_institution = (author.get('last_known_institution') or {}).get('id')
                if last_known_institution is not None:
                    last_known_institution = convert_openalex_id_to_int(last_known_institution)
                author['last_known_institution'] = last_known_institution

                orcid = author['orcid']
                orcid = orcid.replace('https://orcid.org/', '') if orcid is not None else None
                author['orcid'] = orcid

                authors_rows.append(author)
                # authors_writer.writerow(author)

                # ids
                if author_ids := author.get('ids'):
                    author_ids['author_id'] = author_id
                    author_ids['author_name'] = author_name
                    # ids_writer.writerow(author_ids)
                    ids_rows.append(author_ids)

                # counts_by_year
                if counts_by_year := author.get('counts_by_year'):
                    for count_by_year in counts_by_year:
                        count_by_year['author_id'] = author_id
                        count_by_year['author_name'] = author_name
                        counts_by_year_rows.append(count_by_year)
                        # counts_by_year_writer.writerow(count_by_year)

                # concepts
                if x_concepts := author.get('x_concepts'):
                    for x_concept in x_concepts:
                        x_concept['author_id'] = author_id
                        x_concept['author_name'] = author_name
                        x_concept['concept_id'] = convert_openalex_id_to_int(x_concept['id'])
                        x_concept['concept_name'] = x_concept['display_name']

                        # authors_concepts_writer.writerow(x_concept)
                        authors_concepts_rows.append(x_concept)

                # hints
                author_hints_row = {}
                author_name = author['display_name']
                author_hints_row['author_id'] = author_id
                author_hints_row['author_name'] = author_name
                author_hints_row['works_count'] = author.get('works_count', 0)
                author_hints_row['cited_by_count'] = author.get('cited_by_count', 0)
                author_hints_row['most_cited_work'] = author.get('most_cited_work', '')

                # authors_hints_writer.writerow(author_hints_row)
                author_hints_rows.append(author_hints_row)

            ## write all the lines to the CSVs
            authors_writer.writerows(authors_rows)
            ids_writer.writerows(ids_rows)
            counts_by_year_writer.writerows(counts_by_year_rows)
            authors_concepts_writer.writerows(authors_concepts_rows)
            authors_hints_writer.writerows(author_hints_rows)

            finished_files.add(str(jsonl_file_name))
            dump_pickle(obj=finished_files, path=finished_files_pickle_path)

    return


def flatten_authors_concepts(files_to_process: Union[str, int] = 'all'):
    skip_ids = get_skip_ids('authors')
    file_spec = csv_files['authors']
    authors_concepts_zero_filename = CSV_DIR / 'authors_concepts_zero.csv.gz'

    authors_concepts_csv_exists = Path(file_spec['concepts']['name']).exists()
    authors_concepts_zero_csv_exists = authors_concepts_zero_filename.exists()

    with gzip.open(file_spec['concepts']['name'], 'at', encoding='utf-8') as authors_concepts_csv, \
            gzip.open(authors_concepts_zero_filename, 'at', encoding='utf-8') as authors_concepts_zero_csv:

        authors_concepts_writer = csv.DictWriter(
            authors_concepts_csv, fieldnames=file_spec['concepts']['columns'], extrasaction='ignore'
        )
        if not authors_concepts_csv_exists:
            authors_concepts_writer.writeheader()

        authors_concepts_zero_writer = csv.DictWriter(
            authors_concepts_zero_csv, fieldnames=file_spec['concepts']['columns'], extrasaction='ignore'
        )
        if not authors_concepts_zero_csv_exists:
            authors_concepts_zero_writer.writeheader()

        print(f'This might take a while, like 6-7 hours..')

        finished_files_pickle_path = CSV_DIR / 'temp' / 'finished_authors_concepts.pkl'
        finished_files_pickle_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

        if finished_files_pickle_path.exists():
            finished_files = load_pickle(finished_files_pickle_path)  # load the pickle
            print(f'{len(finished_files)} existing files found!')
        else:
            finished_files = set()

        authors_manifest = read_manifest(kind='authors', snapshot_dir=SNAPSHOT_DIR / 'data')

        files = [str(entry.filename) for entry in authors_manifest.entries]
        # files = map(str, glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')))
        files = [f for f in files if f not in finished_files]

        if files_to_process == 'all':
            files_to_process = len(files)
        print(f'{files_to_process=}')

        for i, jsonl_file_name in tqdm(enumerate(files), desc='Flattening author concepts...', unit=' file',
                                       total=len(files)):
            if i > files_to_process:
                break

            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                authors_jsonls = authors_jsonl.readlines()
            author_concept_rows = []
            author_concept_zero_rows = []

            for author_json in tqdm(authors_jsonls, desc='Parsing JSONs', leave=False, unit=' line', unit_scale=True):
                if not author_json.strip():
                    continue

                author = orjson.loads(author_json)

                if not (author_id := author.get('id')):
                    continue
                author_id = convert_openalex_id_to_int(author_id)
                if author_id in skip_ids:
                    continue

                author_name = author['display_name']
                if x_concepts := author.get('x_concepts'):
                    for x_concept in x_concepts:
                        x_concept['author_id'] = author_id
                        x_concept['author_name'] = author_name
                        x_concept['concept_id'] = convert_openalex_id_to_int(x_concept['id'])
                        x_concept['concept_name'] = x_concept['display_name']
                        x_concept['works_count'] = author.get('works_count', 0)
                        x_concept['cited_by_count'] = author.get('cited_by_count', 0)
                        author_concept_rows.append(x_concept)

                        # authors_concepts_writer.writerow(x_concept)

                        if x_concept['level'] == 0:  # store only level 0 concepts here
                            # authors_concepts_zero_writer.writerow(x_concept)
                            author_concept_zero_rows.append(x_concept)

            authors_concepts_writer.writerows(author_concept_rows)
            authors_concepts_zero_writer.writerows(author_concept_zero_rows)

            finished_files.add(str(jsonl_file_name))
            dump_pickle(obj=finished_files, path=finished_files_pickle_path)

    return


def flatten_authors_hints(files_to_process: Union[str, int] = 'all'):
    skip_ids = get_skip_ids('authors')
    file_spec = csv_files['authors']

    authors_hints_csv_exists = Path(file_spec['hints']['name']).exists()

    with gzip.open(file_spec['hints']['name'], 'at', encoding='utf-8') as authors_hints_csv:
        authors_hints_writer = csv.DictWriter(
            authors_hints_csv, fieldnames=file_spec['hints']['columns'], extrasaction='ignore'
        )
        if not authors_hints_csv_exists:
            authors_hints_writer.writeheader()

        print(f'This might take a while, like 6-7 hours..')

        finished_files_pickle_path = CSV_DIR / 'temp' / 'finished_authors_hints.pkl'
        finished_files_pickle_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

        if finished_files_pickle_path.exists():
            finished_files = load_pickle(finished_files_pickle_path)  # load the pickle
            print(f'{len(finished_files)} existing files found!')
        else:
            finished_files = set()

        authors_manifest = read_manifest(kind='authors', snapshot_dir=SNAPSHOT_DIR / 'data')

        files = [str(entry.filename) for entry in authors_manifest.entries]
        # files = map(str, glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')))
        files = [f for f in files if f not in finished_files]

        if files_to_process == 'all':
            files_to_process = len(files)
        print(f'{files_to_process=}')

        for i, jsonl_file_name in tqdm(enumerate(files), desc='Flattening author hints...', unit=' file',
                                       total=len(files)):
            if i > files_to_process:
                break

            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                authors_jsonls = authors_jsonl.readlines()

            author_hints_rows = []
            for author_json in tqdm(authors_jsonls, desc='Parsing JSONs', leave=False, unit=' line', unit_scale=True):
                if not author_json.strip():
                    continue

                author = orjson.loads(author_json)
                # print(f'{author=}')
                # break

                if not (author_id := author.get('id')):
                    continue
                author_id = convert_openalex_id_to_int(author_id)
                if author_id in skip_ids:
                    continue

                author_hints_row = {}
                author_name = author['display_name']
                author_hints_row['author_id'] = author_id
                author_hints_row['author_name'] = author_name
                author_hints_row['works_count'] = author.get('works_count', 0)
                author_hints_row['cited_by_count'] = author.get('cited_by_count', 0)
                author_hints_row['most_cited_work'] = author.get('most_cited_work', '')

                author_hints_rows.append(author_hints_row)

            authors_hints_writer.writerows(author_hints_rows)
            finished_files.add(str(jsonl_file_name))
            dump_pickle(obj=finished_files, path=finished_files_pickle_path)

    return


def process_work_json(skip_ids, jsonl_file_name, finished_files, finished_files_pickle_path):
    """
    Process each work JSON lines file in parallel
    """
    with gzip.open(jsonl_file_name, 'r') as works_jsonl:
        works_jsonls = works_jsonl.readlines()

    work_rows, id_rows, primary_location_rows, location_rows, authorship_rows, biblio_rows = [], [], [], [], [], []
    concept_rows, mesh_rows, oa_rows, refs_rows, rels_rows, abstract_rows = [], [], [], [], [], []

    for work_json in tqdm(works_jsonls, desc=f'Parsing JSONs... {str(Path(jsonl_file_name).parts[-2:])}', unit=' line',
                          unit_scale=True, colour='blue',
                          leave=False):
        if not work_json.strip():
            continue

        work = orjson.loads(work_json)

        if not (work_id := work.get('id')):
            continue

        # works
        work_id = convert_openalex_id_to_int(work_id)
        if work_id in skip_ids:
            continue

        num_authors = 0

        work['work_id'] = work_id
        doi = work['doi']
        doi = doi.replace('https://doi.org/', '') if doi is not None else None
        work['doi'] = doi

        if work['title'] is None:
            title = None
        else:
            title = work['title'].replace(r'\n', ' ')  # deleting stray \n's in title
        work['title'] = title

        # authorships
        if authorships := work.get('authorships'):
            for authorship in authorships:
                if author_id := authorship.get('author', {}).get('id'):
                    num_authors += 1  # increase the count of authors
                    author_id = convert_openalex_id_to_int(author_id)
                    author_name = authorship.get('author', {}).get('display_name')

                    institutions = authorship.get('institutions')
                    institution_ids = [convert_openalex_id_to_int(i.get('id')) for i in institutions]
                    institution_ids = [i for i in institution_ids if i]
                    institution_ids = institution_ids or [None]

                    institution_names = [i.get('display_name') for i in institutions]
                    institution_names = [i for i in institution_names if i]
                    institution_names = institution_names or [None]

                    for institution_id, institution_name in zip(institution_ids, institution_names):
                        authorship_rows.append({
                            # authorships_writer.writerow({
                            'work_id': work_id,
                            'author_position': authorship.get('author_position'),
                            'author_id': author_id,
                            'author_name': author_name,
                            'institution_id': institution_id,
                            'institution_name': institution_name,
                            'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                            'publication_year': work.get('publication_year')
                        })

        # works_writer.writerow(work)
        work['num_authors'] = num_authors
        work_rows.append(work)

        # primary location
        if primary_location := (work.get('primary_location') or {}):
            if source := primary_location.get('source'):
                source_id = convert_openalex_id_to_int(source.get('id'))
                primary_location_rows.append({
                    'work_id': work_id,
                    'source_id': source_id,
                    'source_name': source.get('display_name'),
                    'source_type': source.get('type'),
                    'version': primary_location.get('version'),
                    'license': primary_location.get('license'),
                    'is_oa': primary_location.get('is_oa'),
                })

        # locations
        if locations := work.get('locations'):
            for location in locations:
                if source := location.get('source'):
                    source_id = convert_openalex_id_to_int(source.get('id'))
                    location_rows.append({
                        'work_id': work_id,
                        'source_id': source_id,
                        'source_name': source.get('display_name'),
                        'source_type': source.get('type'),
                        'version': location.get('version'),
                        'license': location.get('license'),
                        'is_oa': location.get('is_oa'),
                    })


        # biblio
        if biblio := work.get('biblio'):
            biblio['work_id'] = work_id
            biblio_rows.append(biblio)
            # biblio_writer.writerow(biblio)

        # concepts
        for concept in work.get('concepts'):
            if concept_id := concept.get('id'):
                concept_id = convert_openalex_id_to_int(concept_id)
                concept_name = concept.get('display_name')
                level = concept.get('level')

                # concepts_writer.writerow({
                concept_rows.append({
                    'work_id': work_id,
                    'publication_year': work.get('publication_year'),
                    'concept_id': concept_id,
                    'concept_name': concept_name,
                    'level': level,
                    'score': concept.get('score'),
                })

        # ids
        if ids := work.get('ids'):
            ids['work_id'] = work_id
            ids['doi'] = doi
            id_rows.append(ids)
            # ids_writer.writerow(ids)

        # mesh
        for mesh in work.get('mesh'):
            mesh['work_id'] = work_id
            mesh_rows.append(mesh)
            # mesh_writer.writerow(mesh)

        # referenced_works
        for referenced_work in work.get('referenced_works'):
            if referenced_work:
                referenced_work = convert_openalex_id_to_int(referenced_work)
                refs_rows.append({
                    # referenced_works_writer.writerow({
                    'work_id': work_id,
                    'referenced_work_id': referenced_work
                })

        # related_works
        for related_work in work.get('related_works'):
            if related_work:
                related_work = convert_openalex_id_to_int(related_work)

                # related_works_writer.writerow({
                rels_rows.append({
                    'work_id': work_id,
                    'related_work_id': related_work
                })

        # abstracts
        if (abstract_inv_index := work.get('abstract_inverted_index')) is not None:
            try:
                abstract = reconstruct_abstract(abstract_inv_index)
            except orjson.JSONDecodeError as e:
                abstract = ''

            abstract_row = {'work_id': work_id, 'title': title, 'abstract': abstract,
                            'publication_year': work.get('publication_year')}
            abstract_rows.append(abstract_row)
            # abstracts_writer.writerow(abstract_row)

    # write the batched parquets here
    kinds = ['works', 'ids', 'primary_location', 'locations', 'authorships', 'biblio', 'concepts', 'mesh',
             'referenced_works', 'related_works', 'abstracts']
    row_names = [work_rows, id_rows, primary_location_rows, location_rows, authorship_rows, biblio_rows,
                 concept_rows, mesh_rows, refs_rows, rels_rows, abstract_rows]

    with tqdm(total=len(kinds), desc='Writing CSVs and parquets', leave=False, colour='green') as pbar:
        for kind, rows in zip(kinds, row_names):
            pbar.set_postfix_str(kind)
            write_to_csv_and_parquet(json_filename=jsonl_file_name, kind=kind, rows=rows)
            pbar.update(1)

    finished_files.add(str(jsonl_file_name))
    dump_pickle(obj=finished_files, path=finished_files_pickle_path)
    return


def flatten_works(files_to_process: Union[str, int] = 'all', threads=1):
    """
    New flattening function that only writes Parquets, uses the Sources
    """
    skip_ids = get_skip_ids('works')

    kinds = ['works', 'ids', 'primary_location', 'locations', 'authorships', 'biblio', 'concepts', 'mesh',
             'referenced_works', 'related_works', 'abstracts']
    for kind in kinds:
        # ensure directories exist
        if kind != 'works':
            kind = f'works_{kind}'
        path = (PARQ_DIR / kind)
        if not path.exists():
            print(f'Creating dir at {str(path)}')
            path.mkdir(parents=True)

    finished_files_pickle_path = PARQ_DIR / 'temp' / 'finished_works.pkl'
    finished_files_pickle_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

    if finished_files_pickle_path.exists():
        finished_files = load_pickle(finished_files_pickle_path)  # load the pickle
        print(f'{len(finished_files)} existing files found!')
    else:
        finished_files = set()

    works_manifest = read_manifest(kind='works', snapshot_dir=SNAPSHOT_DIR / 'data')
    files = [str(entry.filename) for entry in works_manifest.entries]
    files = [f for f in files if f not in finished_files]

    print(f'This might take a while, like 20 hours..')

    if files_to_process == 'all':
        files_to_process = len(files)
    print(f'{files_to_process=}')

    args = []

    for i, jsonl_file_name in tqdm(enumerate(files), desc='Flattening works...', total=files_to_process,
                                   unit=' files'):
        if i >= files_to_process:
            break
        if threads > 1:
            args.append((skip_ids, jsonl_file_name, finished_files, finished_files_pickle_path))
        else:
            process_work_json(skip_ids=skip_ids, jsonl_file_name=jsonl_file_name, finished_files=finished_files,
                              finished_files_pickle_path=finished_files_pickle_path)

    if threads > 1:
        print(f'Spinning up {threads} parallel threads')
        parallel_async(func=process_work_json, args=args, num_workers=threads)

    return


STRING_DTYPE = 'string[pyarrow]'  # use the more memory efficient PyArrow string datatype

if STRING_DTYPE == 'string[pyarrow]':
    assert pd.__version__ >= "1.3.0", f'Pandas version >1.3 needed for String[pyarrow] dtype, have {pd.__version__!r}.'

DTYPES = {
    'works': dict(work_id='int64', doi=STRING_DTYPE, title=STRING_DTYPE, publication_year='Int16',
                  publication_date=STRING_DTYPE,
                  type=STRING_DTYPE, cited_by_count='uint32', num_authors='uint16',
                  is_retracted=STRING_DTYPE, is_paratext=STRING_DTYPE,
                  created_date=STRING_DTYPE, updated_date=STRING_DTYPE),
    'authorships': dict(
        work_id='int64', author_position='category', author_id='Int64', author_name=STRING_DTYPE,
        institution_id='Int64', institution_name=STRING_DTYPE, raw_affiliation_string=STRING_DTYPE,
        publication_year='Int16'),
    'host_venues': dict(
        work_id='int64', venue_id='Int64', venue_name=STRING_DTYPE, type='category', url=STRING_DTYPE, is_oa=float,
        version=STRING_DTYPE,
        license=STRING_DTYPE,
    ),
    'alternate_host_venues': dict(
        work_id='int64', venue_id='Int64', venue_name=STRING_DTYPE, type='category', url=STRING_DTYPE, is_oa=float,
        version=STRING_DTYPE,
        license=STRING_DTYPE
    ),
    'primary_location': dict(
        work_id='int64', source_id='Int64', source_name=STRING_DTYPE, source_type='category', version=STRING_DTYPE,
        license=STRING_DTYPE, is_oa=STRING_DTYPE,
    ),
    'locations': dict(
        work_id='int64', source_id='Int64', source_name=STRING_DTYPE, source_type='category', version=STRING_DTYPE,
        license=STRING_DTYPE, is_oa=STRING_DTYPE,
    ),
    'referenced_works': dict(
        work_id='int64', referenced_work_id='int64'
    ),
    'related_works': dict(
        work_id='int64', related_work_id='int64'
    ),
    'concepts': dict(
        work_id='int64', publication_year='Int16', concept_id='int64', concept_name='category', level='uint8',
        score=float
    ),
    'abstract': dict(
        work_id='int64', publication_year='Int16', title=STRING_DTYPE, abstract=STRING_DTYPE,
    ),
    'ids': dict(
        work_id='int64', openalex=STRING_DTYPE, doi=STRING_DTYPE, mag='Int64', pmid=STRING_DTYPE, pmcid=STRING_DTYPE
    ),
    'biblio': dict(
        work_id='int64', volume=STRING_DTYPE, issue=STRING_DTYPE, first_page=STRING_DTYPE, last_page=STRING_DTYPE,
    )
}


def write_to_csv_and_parquet(rows: list, kind: str, json_filename: str, debug: bool = False,
                             csv_writer: Optional[csv.DictWriter] = None):
    """
    Write rows to the CSV using the CSV writer
    Also create a new file inside the respective parquet directory
    """
    if len(rows) == 0:
        return

    if csv_writer is not None:
        csv_writer.writerows(rows)

    json_filename = Path(json_filename)

    kind_ = f'works_{kind}' if kind != 'works' else 'works'
    parq_filename = PARQ_DIR / kind_ / (
            '_'.join(json_filename.parts[-2:]).replace('updated_date=', '').replace('.gz', '')
            + '.parquet')

    if parq_filename.exists():
        print(f'Parquet already exists {str(parq_filename.parts[-2:])}')
        return
    # parq_filename.parent.mkdir(exist_ok=True, parents=True)
    if debug:
        print(f'{kind=} {parq_filename=} {len(rows)=:,}')

    keep_cols = csv_files['works'][kind]['columns']

    df = (
        pd.DataFrame(rows)
    )
    missing_cols = [col for col in keep_cols if col not in df.columns.tolist()]

    for missing_col in missing_cols:
        df.loc[:, missing_col] = pd.NA

    df = df[keep_cols]

    if kind in DTYPES:
        df = df.astype(dtype=DTYPES[kind], errors='ignore')  # handle pesky dates

    if kind == 'works':
        df = (
            df
            .assign(
                publication_date=lambda df_: pd.to_datetime(df_.publication_date, errors='coerce',
                                                            infer_datetime_format=True),
                created_date=lambda df_: pd.to_datetime(df_.created_date, errors='coerce',
                                                        infer_datetime_format=True),
                updated_date=lambda df_: pd.to_datetime(df_.updated_date, errors='coerce',
                                                        infer_datetime_format=True),
            )
        )
        # don't set the index here
        # df.set_index('work_id', inplace=True)
        # df.sort_values(by='work_id', inplace=True)  # helps with setting the index later

    elif kind == 'authorships':
        df.drop_duplicates(inplace=True)  # weird bug causes authorships table to have repeated rows sometimes

    df.to_parquet(parq_filename, engine='pyarrow', coerce_timestamps='ms', allow_truncated_timestamps=True)
    return


def init_dict_writer(csv_file, file_spec, **kwargs):
    writer = csv.DictWriter(
        csv_file, fieldnames=file_spec['columns'], **kwargs
    )

    if os.stat(file_spec['name']).st_size == 0:  # write header only if file is empty
        writer.writeheader()
    return writer


if __name__ == '__main__':
    # flatten_concepts()  # takes about 30s
    # flatten_venues()  # takes about 20s
    # flatten_institutions()  # takes about 20s

    # files_to_process = 10
    files_to_process = 'all'  # to do everything
    # files_to_process = 100  # or any other number
    threads = 7

    # flatten_authors(files_to_process=files_to_process)  # takes 6-7 hours for the whole thing! ~3 mins per file

    flatten_works(files_to_process=files_to_process, threads=threads)  # takes about 20 hours  ~6 mins per file
