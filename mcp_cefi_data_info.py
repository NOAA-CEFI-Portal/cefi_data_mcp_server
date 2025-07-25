from typing import Optional
import httpx
from mcp.server.fastmcp import FastMCP
import difflib


CEFI_DATA_TREE_URL = "https://psl.noaa.gov/cefi_portal/data_option_json/cefi_data_tree.json"

# Initialize FastMCP server
mcp = FastMCP("cefi_data")

dict_level_options = None
list_level_names = None

def load_cefi_data_tree(url) -> dict:
    """Load the CEFI data tree from the given URL

    Parameters
    ----------
    url : str
        URL to fetch the CEFI data tree JSON.

    Returns
    -------
    dict
        Parsed JSON data from the CEFI data tree.

    """
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(url)
            response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error loading CEFI data tree : {e}")
        return None

def check_cefi_data_cache() -> bool:
    """Check if the CEFI data cache is loaded.

    Returns
    -------
    bool
        True if the cache is loaded, False otherwise.
    """
    global dict_level_options
    global list_level_names
    if dict_level_options is None:
        dict_level_options = load_cefi_data_tree(CEFI_DATA_TREE_URL)['Projects']['CEFI']['regional_mom6']['cefi_portal']
        list_level_names = [
            'region','subdomain','experiment_type','output_frequency','grid_type',
            'release_date','variable_catagory','variable_name','variable_short_name','variable_file_name','file_meta_data'
        ]

    if not dict_level_options:
        return "No CEFI data available currently."

    return True


def is_match(query, candidates):
    """
    Returns the best matched candidate from a list if the query approximately matches it.
    Matching is case-insensitive, partial, or fuzzy. Returns None if no match is found.
    """
    query_lower = query.lower()

    # First try partial matches
    for candidate in candidates:
        if query_lower in candidate.lower():
            return candidate

    # Then try fuzzy matches with a threshold
    best_match = None
    best_score = 0.0

    for candidate in candidates:
        ratio = difflib.SequenceMatcher(None, query_lower, candidate.lower()).ratio()
        if ratio > 0.6 and ratio > best_score:
            best_score = ratio
            best_match = candidate

    return best_match


@mcp.tool()
def get_level_options(
    region:Optional[str]=None,
    subdomain:Optional[str]=None,
    experiment_type:Optional[str]=None,
    output_frequency:Optional[str]=None,
    grid_type:Optional[str]=None,
    release_date:Optional[str]=None,
    variable_catagory:Optional[str]=None
) -> str:
    """provide the options that is shown in the CEFI data tree.
    The function provide options for the given level of the CEFI data tree.
    the arguments top to bottom are the level of the CEFI data tree.
    all argument that is above the level of the given argument should be provided.
    ex: is `subdomain` is provided then `region` should be provided as well.

    Parameters
    ----------
    region : str
        The region for which to show available options.
    subdomain : Optional[str], optional
        The subdomain for which to show available options, by default None.
    experiment_type : Optional[str], optional
        The experiment type for which to show available options, by default None.
    output_frequency : Optional[str], optional
        The output frequency for which to show available options, by default None.
    grid_type : Optional[str], optional
        The grid type for which to show available options, by default None.
    release_date : Optional[str], optional
        The release date for which to show available options, by default None.
    variable_catagory : Optional[str], optional
        The variable category for which to show available options, by default None.

    Returns
    -------
    str
        all avaialable options in a specific level of the CEFI data tree.
    """
    global dict_level_options

    if not check_cefi_data_cache() :
        return "No CEFI data available currently."
    
    if region is None:
        return "\n".join(dict_level_options.keys())

    region = is_match(region, dict_level_options.keys())
    if region is None:
        return "No matching region found."
    

    if subdomain is None:
        return "\n".join(dict_level_options[region].keys())

    subdomain = is_match(subdomain, dict_level_options[region].keys())
    if subdomain is None:
        return "No matching subdomain found."

    if experiment_type is None:
        return "\n".join(dict_level_options[region][subdomain].keys())

    experiment_type = is_match(experiment_type, dict_level_options[region][subdomain].keys())
    if experiment_type is None:
        return "No matching experiment type found."

    if output_frequency is None:
        return "\n".join(dict_level_options[region][subdomain][experiment_type].keys())

    output_frequency = is_match(output_frequency, dict_level_options[region][subdomain][experiment_type].keys())
    if output_frequency is None:
        return "No matching output frequency found."

    if grid_type is None:
        return "\n".join(dict_level_options[region][subdomain][experiment_type][output_frequency].keys())

    grid_type = is_match(grid_type, dict_level_options[region][subdomain][experiment_type][output_frequency].keys())
    if grid_type is None:
        return "No matching grid type found."

    if release_date is None:
        return "\n".join(dict_level_options[region][subdomain][experiment_type][output_frequency][grid_type].keys())

    release_date = is_match(release_date, dict_level_options[region][subdomain][experiment_type][output_frequency][grid_type].keys())
    if release_date is None:
        return "No matching release date found."

    if variable_catagory is None:
        return "\n".join(dict_level_options[region][subdomain][experiment_type][output_frequency][grid_type][release_date].keys())

    variable_catagory = is_match(variable_catagory, dict_level_options[region][subdomain][experiment_type][output_frequency][grid_type][release_date].keys())
    if variable_catagory is None:
        return "No matching variable category found."

    return "\n".join(dict_level_options[region][subdomain][experiment_type][output_frequency][grid_type][release_date][variable_catagory].keys())


@mcp.tool()
def get_level_name() -> str:
    """provide the level category name contains in `list_structure_cache`
    which prepresent the 

    Returns
    -------
    str
        all the level names/meaning corresponding to the keys in each level
        in the CEFI data tree `dict_level_options`.
    """
    global list_level_names

    if not check_cefi_data_cache() :
        return "No CEFI data available currently."
    
    return "All the level category name from top to bottom\n"+"\n".join(list_level_names)


if __name__ == "__main__":
    # preload the CEFI data tree
    check_cefi_data_cache()
    # Initialize and run the server
    mcp.run(transport='stdio')
