import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

def clean_dataframe(df):
    """
    Clean the dataframe based on the provided Jupyter notebook logic
    """
    try:
        # Rename columns for clarity
        columns_mapping = {
            'SEQGEN23': 'Generator file sequence number',
            'YEAR': 'Data Year',
            'PSTATEABB': 'Plant state abbreviation', 
            'PNAME': 'Plant name',
            'ORISPL': 'DOE/EIA ORIS plant or facility code',
            'GENID': 'Generator ID',
            'NUMBLR': 'Number of associated boilers',
            'GENSTAT': 'Generator status',
            'PRMVR': 'Generator prime mover type',
            'FUELG1': 'Generator primary fuel',
            'NAMEPCAP': 'Generator nameplate capacity (MW)',
            'CFACT': 'Generator capacity factor',
            'GENNTAN': 'Generator annual net generation (MWh)',
            'GENNTOZ': 'Generator ozone season net generation (MWh)',
            'GENERSRC': 'Generation data source',
            'GENYRONL': 'Generator year on-line',
            'GENYRRET': 'Generator planned or actual retirement year'
        }
        
        # Check if columns exist before renaming
        existing_columns = set(df.columns).intersection(set(columns_mapping.keys()))
        rename_dict = {col: columns_mapping[col] for col in existing_columns}
        
        if rename_dict:
            df = df.rename(columns=rename_dict)
        
        # If columns are already in the new format, don't try to rename them
        else:
            logger.info("Columns seem to be already renamed or in a different format")
            
        # Handle missing values with appropriate defaults
        fill_values = {
            'Number of associated boilers': 0,
            'Generator nameplate capacity (MW)': 0,
            'Generator capacity factor': 0,
            'Generator annual net generation (MWh)': 0,
            'Generator ozone season net generation (MWh)': 0,
            'Generator planned or actual retirement year': 0,
            'Generator status': 'Unknown',
            'Generator prime mover type': 'Unknown',
            'Generator primary fuel': 'Unknown',
            'Generation data source': 'Unknown'
        }
        
        # Only fill columns that exist
        fill_dict = {k: v for k, v in fill_values.items() if k in df.columns}
        if fill_dict:
            df.fillna(fill_dict, inplace=True)
        
        # Remove duplicates
        df.drop_duplicates(inplace=True)
        
        # Convert data types
        type_conversions = {
            'Data Year': 'int',
            'Generator nameplate capacity (MW)': 'float',
            'Generator capacity factor': 'float',
            'Generator annual net generation (MWh)': 'float',
            'Generator ozone season net generation (MWh)': 'float',
            'Generator year on-line': 'int',
            'Generator planned or actual retirement year': 'int'
        }
        
        # Only convert columns that exist
        for col, dtype in type_conversions.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype, errors='ignore')
                except Exception as e:
                    logger.warning(f"Could not convert {col} to {dtype}: {e}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error cleaning dataframe: {e}")
        # Return original dataframe if cleaning fails
        return df

def clean_csv_data(file_content):
    """
    Process and clean CSV data from uploaded file
    """
    try:
        # Read CSV file
        df = pd.read_csv(BytesIO(file_content), encoding='utf-8')
        
        # Clean the data
        cleaned_df = clean_dataframe(df)
        
        return cleaned_df
    
    except Exception as e:
        logger.error(f"Error processing CSV data: {e}")
        # Return empty dataframe on error
        return pd.DataFrame()

def clean_excel_data(file_content, sheet_name='GEN23'):
    """
    Process and clean Excel data from uploaded file
    """
    try:
        # Read Excel file
        df = pd.read_excel(BytesIO(file_content), sheet_name=sheet_name)
        
        # Clean the data
        cleaned_df = clean_dataframe(df)
        
        return cleaned_df
    
    except Exception as e:
        logger.error(f"Error processing Excel data: {e}")
        # Return empty dataframe on error
        return pd.DataFrame()

def convert_to_api_format(df):
    """
    Convert the cleaned dataframe to the format expected by the API
    """
    try:
        # Create a mapping from cleaned column names to API column names
        api_columns = {
            'Generator ID': 'GENID',
            'Plant name': 'PNAME',
            'Plant state abbreviation': 'PSTATEABB',
            'DOE/EIA ORIS plant or facility code': 'ORISPL',
            'Generator annual net generation (MWh)': 'GENNTAN'
        }
        
        # Create a new dataframe with only the needed columns
        api_df = pd.DataFrame()
        
        for clean_col, api_col in api_columns.items():
            if clean_col in df.columns:
                api_df[api_col] = df[clean_col]
            # If we already have the API column format, use it directly
            elif api_col in df.columns:
                api_df[api_col] = df[api_col]
        
        # Ensure all required columns exist
        for api_col in api_columns.values():
            if api_col not in api_df.columns:
                raise ValueError(f"Required column {api_col} not found in data")
                
        return api_df
    
    except Exception as e:
        logger.error(f"Error converting to API format: {e}")
        return pd.DataFrame() 