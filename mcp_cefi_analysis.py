from typing import Literal, Optional
from mcp.server.fastmcp import FastMCP
import xarray as xr
import fsspec

# Initialize FastMCP server
mcp = FastMCP("cefi_analysis")

# Get the data from the kerchunk index file located in the cloud storage
def get_cloud_data(
    object_link_kerchunk_index:str,
    cloud_options:Literal["s3","gcs"] = "s3"
)->xr.Dataset:
    """
    Get the dataset based on the kerchunk index file located at the
    cloud storage and lazy loaded the dataset as and virtual zarr store.

    Parameters
    ----------
    object_link_kerchunk_index : str
        The link to the kerchunk index file in the cloud storage.
    cloud_options : Literal["s3", "gcs"]
        The type of cloud storage to use, either "s3" for Amazon S3 
        or "gcs" for Google Cloud Storage.
    
    Returns
    -------
    xr.Dataset
        An xarray Dataset containing the data from the kerchunk index file.
    """
    try:
        # using fsspec to read the kerchunk index file
        fs = fsspec.filesystem(
            "reference",
            fo=object_link_kerchunk_index,
            remote_protocol=cloud_options,
            remote_options={"anon":True},
            skip_instance_cache=True,
            target_options={"anon": True}
        )

        ds = xr.open_dataset(fs.get_mapper(), engine='zarr', consolidated=False)
        return ds

    except Exception as e:
        print(f"Error occurred while accessing cloud storage: {e}")
        return None

# Get the data from the kerchunk index file located in the cloud storage
def get_opendap_data(
    opendap_url:str
)->xr.Dataset:
    """
    Get the dataset based on the OPeNDAP url provided.

    Parameters
    ----------
    opendap_url : str
        The OPeNDAP URL to the dataset.
    
    Returns
    -------
    xr.Dataset
        An xarray Dataset containing the data from the OPeNDAP link.
    """
    # using fsspec to read the kerchunk index file
    try:
        ds = xr.open_dataset(opendap_url)
        return ds

    except Exception as e:
        print(f"Error occurred while accessing OPeNDAP URL: {e}")
        return None

def get_available_data(
        opendap_url:Optional[str]=None,
        s3_object_link_kerchunk_index:Optional[str]=None,
        gcs_object_link_kerchunk_index:Optional[str]=None
)-> xr.Dataset:
    """
    The function get the data from the available options
    in the following orders
    1. opendap
    2. s3 bucket
    3. gcs bucket
    
    Parameters
    ----------
    opendap_url : Optional[str]
        The OPeNDAP URL to the dataset.
    s3_object_link_kerchunk_index : Optional[str]
        The S3 object link to the kerchunk index file.
    gcs_object_link_kerchunk_index : Optional[str]
        The GCS object link to the kerchunk index file.
    
    Returns
    -------
    xr.Dataset
        An xarray Dataset containing the data from the available source.
    """
    if not opendap_url and not s3_object_link_kerchunk_index and not gcs_object_link_kerchunk_index:
        raise ValueError(
            "At least one of the parameters must be provided: "+
            "opendap_url, s3_object_link_kerchunk_index, gcs_object_link_kerchunk_index"
        )

    # try opendap first
    if isinstance(opendap_url,str):
        ds = get_opendap_data(opendap_url)
        return ds

    # if OPeNDAP fails, try cloud storage S3
    if isinstance(s3_object_link_kerchunk_index,str):
        ds = get_cloud_data(
            s3_object_link_kerchunk_index,
            cloud_options="s3"
        )
        return ds

    # if above two fails, try cloud storage GCS
    if gcs_object_link_kerchunk_index:
        ds = get_cloud_data(
            gcs_object_link_kerchunk_index,
            cloud_options="gcs"
        )
        return ds
    
    return None

@mcp.tool()
def get_file_metadata(
    opendap_url:str,
    s3_object_link_kerchunk_index:str,
    gcs_object_link_kerchunk_index:str
) -> dict:
    """
    Get the metadata of the dataset from the OPeNDAP URL or cloud storage.

    Parameters
    ----------
    opendap_url : str
        The OPeNDAP URL to the dataset.
    s3_object_link_kerchunk_index : str
        The S3 object link to the kerchunk index file.
    gcs_object_link_kerchunk_index : str
        The GCS object link to the kerchunk index file.
    
    Returns
    -------
    xr.Dataset
        An xarray Dataset containing the metadata of the dataset.
    """
    ds = get_available_data(
        opendap_url,
        s3_object_link_kerchunk_index,
        gcs_object_link_kerchunk_index
    )

    meta_data = ds.attrs

    return meta_data



if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
