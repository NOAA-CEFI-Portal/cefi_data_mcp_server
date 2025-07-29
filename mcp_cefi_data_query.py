import httpx
import json
from mcp.server.fastmcp import FastMCP
# from cefi_thredds_crawl import find_all_files_thredds, CEFI_THREDDS_BASE

CEFI_DATA_TREE_URL = "https://psl.noaa.gov/cefi_portal/data_option_json/cefi_data_tree.json"

# preload the CEFI opendap available catalog
# dict_catalog = find_all_files_thredds(CEFI_THREDDS_BASE)

# Initialize FastMCP server
mcp = FastMCP("cefi_data")

dict_data_tree = None

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
    global dict_data_tree

    if dict_data_tree is None:
        dict_data_tree = load_cefi_data_tree(CEFI_DATA_TREE_URL)['Projects']['CEFI']['regional_mom6']['cefi_portal']

    return True

@mcp.tool()
def get_region_options() -> str:
    """Get the available regions in the CEFI data tree.

    Returns
    -------
    str
        A string of available regions.
    """
    global dict_data_tree
    regions = []
    for region in dict_data_tree.keys():
        regions.append(region)

    return "\n".join(regions)

@mcp.tool()
def get_subdomain_options(region) -> str:
    """Get the available subdomain in the CEFI data tree
    with a given region.

    Parameters
    ----------
    region : str
        The region for which to get the subdomains.

    Returns
    -------
    str
        A string of available subdomain.
    """
    global dict_data_tree
    subdomains = []
    for subdomain in dict_data_tree[region].keys():
        subdomains.append(subdomain)

    return "\n".join(subdomains)

@mcp.tool()
def get_experiment_options(region,subdomain) -> str:
    """Get the available experiemnt type in the CEFI data tree.

    Parameters
    ----------
    region : str
        The region for which to get the experiment types.
    subdomain : str
        The subdomain for which to get the experiment types.

    Returns
    -------
    str
        A string of available experiment type.
    """
    global dict_data_tree
    experiment_types = []
    for experiment_type in dict_data_tree[region][subdomain].keys():
        experiment_types.append(experiment_type)

    return "\n".join(experiment_types)

@mcp.tool()
def get_output_frequency_options(region,subdomain,experiment_type) -> str:
    """Get the available output frequency in the CEFI data tree.

    Parameters
    ----------
    region : str
        The region for which to get the output frequencies.
    subdomain : str
        The subdomain for which to get the output frequencies.
    experiment_type : str
        The experiment type for which to get the output frequencies.

    Returns
    -------
    str
        A string of available output frequency.
    """
    global dict_data_tree
    output_frequencies = []
    for output_frequency in dict_data_tree[region][subdomain][experiment_type].keys():
        output_frequencies.append(output_frequency)

    return "\n".join(output_frequencies)

@mcp.tool()
def get_grid_type_options(region,subdomain,experiment_type,output_frequency) -> str:
    """Get the available grid type in the CEFI data tree.

    Parameters
    ----------
    region : str
        The region for which to get the grid types.
    subdomain : str
        The subdomain for which to get the grid types.
    experiment_type : str
        The experiment type for which to get the grid types.
    output_frequency : str
        The output frequency for which to get the grid types.

    Returns
    -------
    str
        A string of available grid type.
    """
    global dict_data_tree
    grid_types = []
    for grid_type in dict_data_tree[region][subdomain][experiment_type][output_frequency].keys():
        grid_types.append(grid_type)

    return "\n".join(grid_types)

@mcp.tool()
def get_release_date_options(region,subdomain,experiment_type,output_frequency,grid_type) -> str:
    """Get the available release date in the CEFI data tree.

    Parameters
    ----------
    region : str
        The region for which to get the release dates.
    subdomain : str
        The subdomain for which to get the release dates.
    experiment_type : str
        The experiment type for which to get the release dates.
    output_frequency : str
        The output frequency for which to get the release dates.
    grid_type : str
        The grid type for which to get the release dates.

    Returns
    -------
    str
        A string of available release date.
    """
    global dict_data_tree
    release_dates = []
    for release_date in dict_data_tree[region][subdomain][experiment_type][output_frequency][grid_type].keys():
        release_dates.append(release_date)

    return "\n".join(release_dates)

@mcp.tool()
def get_variable_category_options(region,subdomain,experiment_type,output_frequency,grid_type,release_date) -> str:
    """Get the available variable category in the CEFI data tree.
    
    Parameters
    ----------
    region : str
        The region for which to get the variable categories.
    subdomain : str
        The subdomain for which to get the variable categories.
    experiment_type : str
        The experiment type for which to get the variable categories.
    output_frequency : str
        The output frequency for which to get the variable categories.
    grid_type : str
        The grid type for which to get the variable categories.
    release_date : str
        The release date for which to get the variable categories.

    Returns
    -------
    str
        A string of available category.
    """
    global dict_data_tree
    categories = []
    for category in dict_data_tree[region][subdomain][experiment_type][output_frequency][grid_type][release_date].keys():
        categories.append(category)

    return "\n".join(categories)

@mcp.tool()
def get_variable_name_options(region,subdomain,experiment_type,output_frequency,grid_type,release_date,variable_catagory) -> str:
    """Get the available variable name in the CEFI data tree.

    Parameters
    ----------
    region : str
        The region for which to get the variable names.
    subdomain : str
        The subdomain for which to get the variable names.
    experiment_type : str
        The experiment type for which to get the variable names.
    output_frequency : str
        The output frequency for which to get the variable names.
    grid_type : str
        The grid type for which to get the variable names.
    release_date : str
        The release date for which to get the variable names.
    variable_catagory : str
        The variable category for which to get the variable names.

    Returns
    -------
    str
        A string of available variables.
    """
    global dict_data_tree
    variable_long_names = []
    variable_short_names = []
    variable_filenames = []
    for variable_long in dict_data_tree[region][subdomain][experiment_type][output_frequency][grid_type][release_date][variable_catagory].keys():
        variable_long_names.append(variable_long)
        for variable_short in dict_data_tree[region][subdomain][experiment_type][output_frequency][grid_type][release_date][variable_catagory][variable_long].keys():
            variable_short_names.append(variable_short)
            for variable_filename in dict_data_tree[region][subdomain][experiment_type][output_frequency][grid_type][release_date][variable_catagory][variable_long][variable_short].keys():
                variable_filenames.append(variable_filename)

    all_long_names = "All available variable full name\n"+"\n".join(variable_long_names)
    all_short_names = "All available variable short name\n"+"\n".join(variable_short_names)
    all_filenames = "All available variable filename\n"+"\n".join(variable_filenames)

    return f"{all_long_names}\n\n{all_short_names}\n\n{all_filenames}"

def general_url_format(
    region:str,
    subdomain:str,
    experiment_type:str,
    output_frequency:str,
    grid_type:str,
    release_date:str,
    variable_name_ncfile:str
) -> str:
    """Generate a general URL format for accessing CEFI data.
    the general URL format is:
    {region}/{subdomain}/{experiment_type}/{output_frequency}/{grid_type}/{release_date}/{variable_name_ncfile}

    Returns
    -------
    str
        all the level names/meaning corresponding to the keys in each level
        in the CEFI data tree `dict_level_options`.
    """
    
    return f"{region}/{subdomain}/{experiment_type}/{output_frequency}/{grid_type}/{release_date}/{variable_name_ncfile}"

@mcp.tool()
def get_opendap_url(
    region:str,
    subdomain:str,
    experiment_type:str,
    output_frequency:str,
    grid_type:str,
    release_date:str,
    variable_name_ncfile:str
) -> str:
    """
    Get the OPeNDAP URL for the specified CEFI data.
    
    Parameters
    ----------
    region : str
        The region for which to get the OPeNDAP URL.
    subdomain : str         
        The subdomain for which to get the OPeNDAP URL.
    experiment_type : str
        The experiment type for which to get the OPeNDAP URL.
    output_frequency : str
        The output frequency for which to get the OPeNDAP URL.
    grid_type : str
        The grid type for which to get the OPeNDAP URL.
    release_date : str
        The release date for which to get the OPeNDAP URL.
    variable_name_ncfile : str
        The filename for which to get the OPeNDAP URL.

    Returns
    -------
    str
        The OPeNDAP URL for the specified CEFI data.
    """
    
    # Base opendap url
    base_opendap_url = "http://psl.noaa.gov/thredds/dodsC/Projects/CEFI/regional_mom6/cefi_portal/"

    # Get the general URL format
    general_url = general_url_format(
        region,
        subdomain,
        experiment_type,
        output_frequency,
        grid_type,
        release_date,
        variable_name_ncfile
    )

    return f"OPeNDAP URL : {base_opendap_url}{general_url}"

if __name__ == "__main__":
    check_cefi_data_cache()
    # Initialize and run the server
    mcp.run(transport='stdio')
