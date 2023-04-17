from seaborn._statistics import EstimateAggregator
import pandas as pd
from collections import namedtuple
import numpy as np
import math 
import string 


def conf_interval(data, aggfunc='mean', errorfunc=('ci', 95), 
                  return_errors=False):
    """
    data: set of values, can be a pandas series or numpy array like  
    aggfunc: function to aggregate: mean/median 
    errorfunc: ('ci', 95), 'sd' (standard dev), 'se' (standard error)
    return_errors: return difference between the means if True, else return the absolute boundaries 
    """
    data = np.array(data)
    data = data[~np.isnan(data)]   # remove NaNs
    
    if len(data) == 0:
        res = namedtuple('result', 'y ymin ymax')
        res.y = np.nan
        res.ymin = np.nan
        res.ymax = np.nan
    else:
        agg = EstimateAggregator(aggfunc, errorfunc)
        df = pd.DataFrame({'y': data})
        res = agg(df, 'y')
    
    result = res.y
    errorfunc_name = aggfunc + '_' + ''.join(map(str, errorfunc))
    
    if return_errors:
        y_error_min, y_error_max =  result - res.ymin, res.ymax - result

        ser = pd.Series({aggfunc: result, 
                         f'{errorfunc_name}_error_min': y_error_min,
                         f'{errorfunc_name}_error_max': y_error_max})
    else:
        ser = pd.Series({aggfunc: result,
                         f'{errorfunc_name}_min': res.ymin,
                         f'{errorfunc_name}_max': res.ymax})
    return ser 


def sanitize_str(s):
    """
    Remove spaces, make camelcase 
    """
    s = ''.join(map(lambda x: x.title(), s.split()))
    
    s = s.translate(str.maketrans('', '', string.punctuation))
    if s == 'Imp1':
        s = 'ImpOne'
    return s
    

def format_p_value(p):
    '''
    format each p value as 
    ns      P > 0.05
    *       P ≤ 0.05
    **      P ≤ 0.01
    ***     P ≤ 0.001
    ****    P ≤ 0.0001
    '''
    p = float(p)
    if p <= 0.0001:
        st = '(****)'
        return st
    elif p <= 0.001:
        st = '(***)'
        return st
    elif p <= 0.01:
        st = '(**)'
    elif p <= 0.05:
        st = '(*)'
    else:
        st = ''
    return f'p={p:.3F} {st}'
    
    
def conversational_number(number):
    words = {
        "1.0": "one",
        "2.0": "two",
        "3.0": "three",
        "4.0": "four",
        "5.0": "five",
        "6.0": "six",
        "7.0": "seven",
        "8.0": "eight",
        "9.0": "nine",
    }

    if number < 1:
        return round(number, 2)

    elif number < 1000:
        return int(math.floor(number))

    elif number < 1000000:
        divided = number / 1000
        unit = "K"

    else:
        divided = number / 1_000_000
        unit = "M"

    short_number = '{}'.format(round(divided, 1))[:-2]

    return short_number + unit


def annotate_plot(x, y, label_attr, data, ax, color='gray', orient='above'):
    """
    Annotate the plot with label attr at (x, y)
    """
    if orient == 'above':
        args = dict(
            xytext=(-2, 15),
        )
    else:
        args = dict(
            xytext=(-2, -15)
        )
    labels_dict = dict(zip(data[x], data[label_attr]))
    for x, y in zip(data[x], data[y]):
        label = conversational_number(labels_dict[x])
        ax.annotate(label, # this is the text
                    (x, y), # these are the coordinates to position the label
                    textcoords='offset points', # how to position the text
                    color=color,
                    va='center',
                    ha='left',
                    fontsize=15, **args) # horizontal alignment can be left, right or center
    return ax


def plot_error_bars(data, ax, x, y, ecolor, errors, jitter=0, annot=False):
    """
    jitter makes the scatter points not overlap
    annot - annotates the plot based on whether the CI of the diff contains 0
    """
    # ax.invert_yaxis()
    mean_var = x
    min_var, max_var = f'{mean_var}_{errors}_min', f'{mean_var}_{errors}_max'
    
    x_coords, y_coords = [], []
        
    dots = ax.collections[-1]
    offsets = dots.get_offsets()
    jittered_offsets = offsets + [0, -jitter]
    dots.set_offsets(jittered_offsets)
    
    for x_, y_ in dots.get_offsets():
        x_coords.append(x_)
        y_coords.append(y_)
    
    error = [data[mean_var] - data[min_var], data[max_var] - data[mean_var]]

    ax.errorbar(x_coords, y_coords, xerr=error, ecolor=ecolor, fmt='none', capsize=8, elinewidth=5, alpha=0.4, capthick=4)
    
    good = None 
    ## check if difference error bars contain 0 
    if 'diff' in  x:
        error_prod = data[min_var] * data[max_var]
        good = 0
        
        X = max(min(x_coords) - 0.02, -0.1)
        yrange = np.linspace(0.05, 1, num=len(data[x]))
        for err_p, x, y in zip(error_prod, x_coords, y_coords):
            if err_p < 0:  # 0 is contained 
                text = ''
            else:
                text = '(*)'
                good += 1
            if annot:
                ax.annotate(
                    text, 
                    va='center', ha='left', 
                    xy=(x, y), xycoords='data', color='green',
                    xytext=(-75, 0), textcoords='offset points',
                    annotation_clip=False,
                )
    
    if good is not None:
        ax.set_title(ax.get_title() + f' ({good}/{len(data[mean_var])})')
    return ax 


def plot_error_bands(x, y, errors, data, facecolor, ax, alpha=0.2):
    """
    errors: name of error function, se or ci95
    """
    err_min, err_max = data[f'{y}_{errors}_min'], data[f'{y}_{errors}_max']
    ax.fill_between(data[x], err_min, err_max, alpha=alpha, facecolor=facecolor, antialiased=True);
    return ax
    

def get_topic_list(discipline):
    if discipline == 'Physics':
        return [
            'Gravitational wave',
            'Dark matter',
            'Fluid dynamics',
            'Soliton',
            'Supersymmetry',
            'Statistical physics',          
            'Superconductivity' 
        ]
    elif discipline == 'CS':
        return [
            'Compiler',
            'Mobile computing',
            'Cryptography',
            'Cluster analysis', 
            'Image processing',
            'Parallel computing'         
        ]
    elif discipline == 'BioMed':
        return [
            'Protein structure',
            'Genome', 
            'Peptide sequence',
            "Alzheimer's disease",
            'Neurology',          
            'Radiation therapy',
            'Chemotherapy'
        ]
    else:
        raise NotImplementedError(f'Invalid {discipline=}. Pick from Physics, CS, and BioMed.')

def read_parquet(name, **args):
    path = datapath / f'{name}.parquet'
    df = pd.read_parquet(path, engine='pyarrow')
            
    print(f'Read {len(df):,} rows from {path.stem!r}')
    return df         
