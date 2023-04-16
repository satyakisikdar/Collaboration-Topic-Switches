import pickle
from datetime import datetime
import ujson as json
from box import Box
from multiprocessing import Pool


def load_pickle(path):
    with open(path, 'rb') as reader:
        return pickle.load(reader)


def dump_pickle(obj, path):
    with open(path, 'wb') as writer:
        pickle.dump(obj, writer)


def convert_openalex_id_to_int(openalex_id):
    if not openalex_id:
        return None
    try:
        openalex_id = openalex_id.strip().replace('https://openalex.org/', '')
        id_ = int(openalex_id[1:])
    except Exception:
        id_ = None
    return id_


def parallel_async(func, args, num_workers: int):
    def update_result(result):
        return result

    results = []
    async_promises = []
    with Pool(num_workers) as pool:
        for arg in args:
            r = pool.apply_async(func, arg, callback=update_result)
            async_promises.append(r)
        for r in async_promises:
            try:
                r.wait()
                results.append(r.get())
            except Exception as e:
                results.append(r.get())

    return results


def reconstruct_abstract(inv_abstract_st):
    if inv_abstract_st is None:
        return ''

    if isinstance(inv_abstract_st, bytes):
        inv_abstract_st = inv_abstract_st.decode('utf-8', errors='replace')
    # inv_abstract_st = ast.literal_eval(inv_abstract_st)  # convert to python object

    inv_abstract = json.loads(inv_abstract_st) if isinstance(inv_abstract_st, str) else inv_abstract_st
    abstract_dict = {}
    for word, locs in inv_abstract.items():  # invert the inversion
        for loc in locs:
            abstract_dict[loc] = word
    abstract = ' '.join(map(lambda x: x[1],  # pick the words
                            sorted(abstract_dict.items())))  # sort abstract dictionary by indices
    if len(abstract) == 0:
        abstract = ''
    return abstract


def read_manifest(kind: str, snapshot_dir) -> Box:
    manifest_path = snapshot_dir / kind / 'manifest'
    create_date = datetime.fromtimestamp(manifest_path.stat().st_ctime).strftime("%a, %b %d %Y")

    raw_data = Box(json.load(open(manifest_path)))
    print(f'Reading {kind!r} manifest created on {create_date}. {len(raw_data.entries):,} files, '
          f'{raw_data.meta.record_count:,} records.')
    data = Box({'len': raw_data.meta.record_count})

    entries = []
    for raw_entry in raw_data.entries:
        filename = snapshot_dir / raw_entry.url.replace('s3://openalex/data/', '')
        entry = Box({'filename': filename, 'kind': kind,
                     'count': raw_entry.meta.record_count,
                     'updated_date': '_'.join(filename.parts[-2:]).replace('.gz', '')})
        entries.append(entry)

    data['entries'] = sorted(entries, key=lambda x: x.count)

    return data
