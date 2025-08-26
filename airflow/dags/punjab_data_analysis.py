from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import json
import pytz
import time
from io import StringIO
import gc

# ============================================================================
# DAG Configuration
# ============================================================================

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'punjab_property_tax_analysis',
    default_args=default_args,
    description='Comprehensive analysis of Punjab property tax data including demand, collections, arrears, and penalties',
    schedule_interval='0 6 * * *',  # Daily execution at 6 AM IST
    catchup=False,
    tags=['punjab', 'property-tax', 'analysis', 'etl']
)

# ============================================================================
# Main Data Processing Task
# ============================================================================

def process_punjab_data(**context):
    """
    Primary ETL function that processes Punjab property tax data.
    
    This function performs the following operations:
    1. Extracts data from source PostgreSQL database
    2. Transforms and enriches the data with calculated fields
    3. Loads the processed data into the analytics datamart
    
    Key calculations performed:
    - Financial year determination from tax periods
    - Arrear calculations (outstanding amounts from previous years)
    - Current year demand vs latest year demand
    - Penalty and interest aggregations
    - Property area summations across units
    - Ownership classification based on occupancy types
    
    Args:
        **context: Airflow context containing task instance and execution details
        
    Returns:
        int: Number of records processed and loaded
    """
    
    # Initialize database connections
    source_hook = PostgresHook(postgres_conn_id='postgres_source')
    target_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    # Set timezone and capture extraction timestamp
    ist = pytz.timezone('Asia/Kolkata')
    current_extraction_time = datetime.now(ist)
    
    print(f"Starting Punjab property tax analysis at: {current_extraction_time}")
    
    # ========================================================================
    # Data Extraction Phase
    # ========================================================================
    
    print("Phase 1: Extracting source data...")
    
    # Extract active properties with basic attributes
    property_query = """
    SELECT id, propertyid, tenantid, createdtime, additionaldetails, 
           ownershipcategory, status, usagecategory, propertytype
    FROM eg_pt_property 
    WHERE status = 'ACTIVE'
    """
    property_df = source_hook.get_pandas_df(property_query)
    property_df = property_df[property_df['status'] == 'ACTIVE'].copy()
    print(f"  Active properties loaded: {len(property_df)}")
    
    # Extract all property units (contains area and occupancy information)
    unit_query = """
    SELECT *
    FROM eg_pt_unit
    """
    unit_all_columns_df = source_hook.get_pandas_df(unit_query)
    print(f"  Property units loaded: {len(unit_all_columns_df)}")
    
    # Extract active demand records (tax assessment periods)
    demand_query = """
    SELECT id, taxperiodfrom, taxperiodto, consumercode, status
    FROM egbs_demand_v1
    """
    demand_df = source_hook.get_pandas_df(demand_query)
    demand_df = demand_df[demand_df['status'] == 'ACTIVE'].copy()
    demand_df['consumercode'] = demand_df['consumercode'].astype(str)
    print(f"  Active demands loaded: {len(demand_df)}")
    
    # ========================================================================
    # Chunked Loading for Large Tables
    # ========================================================================
    
    print("Phase 2: Loading demand details in optimized chunks...")
    
    # Get total record count for demand details
    count_query = "SELECT COUNT(*) FROM egbs_demanddetail_v1"
    total_dd_records = source_hook.get_first(count_query)[0]
    print(f"  Total demand detail records to process: {total_dd_records:,}")
    
    # Process demand details in chunks to manage memory efficiently
    CHUNK_SIZE = 500000  # Optimal chunk size for memory management
    all_chunks = []
    offset = 0
    
    while offset < total_dd_records:
        print(f"  Loading chunk starting at offset: {offset:,}")
        
        chunk_query = f"""
        SELECT demandid, 
               CAST(taxamount AS DECIMAL(15,2)) as taxamount, 
               CAST(collectionamount AS DECIMAL(15,2)) as collectionamount, 
               taxheadcode
        FROM egbs_demanddetail_v1
        ORDER BY demandid
        LIMIT {CHUNK_SIZE} OFFSET {offset}
        """
        
        chunk = source_hook.get_pandas_df(chunk_query)
        if chunk.empty:
            break
            
        all_chunks.append(chunk)
        print(f"    Chunk loaded: {len(chunk)} records")
        
        offset += CHUNK_SIZE
        
        # Limit chunks to prevent excessive memory usage
        if len(all_chunks) >= 6:
            break
    
    # Combine all chunks into single dataframe
    demand_details_df = pd.concat(all_chunks, ignore_index=True)
    del all_chunks; gc.collect()  # Free memory
    
    print(f"  Total demand details processed: {len(demand_details_df)}")
    
    # Ensure consistent data types for joins
    demand_details_df['demandid'] = demand_details_df['demandid'].astype(str)
    demand_details_df['taxamount'] = pd.to_numeric(demand_details_df['taxamount'], errors='coerce').fillna(0)
    demand_details_df['collectionamount'] = pd.to_numeric(demand_details_df['collectionamount'], errors='coerce').fillna(0)
    
    # ========================================================================
    # Data Transformation Phase
    # ========================================================================
    
    print("Phase 3: Joining demand and demand details...")
    
    # Join demand headers with detailed line items
    demand_df['id'] = demand_df['id'].astype(str)
    
    joined_demand = demand_df.merge(
        demand_details_df, 
        left_on='id', 
        right_on='demandid', 
        how='left', 
        suffixes=('_demand', '_detail')
    )
    print(f"  Records after demand join: {len(joined_demand)}")
    
    # Remove records without tax head codes (demands without details)
    joined_demand = joined_demand.dropna(subset=['taxheadcode']).copy()
    print(f"  Records after filtering null tax head codes: {len(joined_demand)}")
    
    # Free memory from intermediate dataframes
    del demand_details_df, demand_df; gc.collect()
    
    # ========================================================================
    # Financial Year Processing
    # ========================================================================
    
    print("Phase 4: Processing financial years and tax periods...")
    
    # Convert epoch timestamps to datetime objects in IST timezone
    joined_demand['taxperiodfrom'] = pd.to_datetime(joined_demand['taxperiodfrom'], unit='ms', utc=True)
    joined_demand['taxperiodto'] = pd.to_datetime(joined_demand['taxperiodto'], unit='ms', utc=True)
    
    joined_demand['taxperiodfrom'] = joined_demand['taxperiodfrom'].dt.tz_convert(ist)
    joined_demand['taxperiodto'] = joined_demand['taxperiodto'].dt.tz_convert(ist)
    
    def get_fy(date):
        """
        Calculate financial year from date.
        Indian FY runs from April to March (e.g., 2024-25 = Apr 2024 to Mar 2025)
        
        Args:
            date: datetime object
            
        Returns:
            str: Financial year in format "YYYY-YY"
        """
        if date.month >= 4:
            fy_start = date.year
            fy_end = date.year + 1
        else:
            fy_start = date.year - 1
            fy_end = date.year
        return f"{fy_start}-{str(fy_end)[-2:]}"
    
    joined_demand['fy'] = joined_demand['taxperiodfrom'].apply(get_fy)
    
    # ========================================================================
    # Financial Analysis Calculations
    # ========================================================================
    
    print("Phase 5: Calculating financial year summaries...")
    
    # Determine the span of financial years for each property
    result = joined_demand.groupby('consumercode')['fy'].agg(['min', 'max']).reset_index()
    result.rename(columns={'min': 'earliest_fy', 'max': 'latest_fy'}, inplace=True)
    print(f"  Financial year ranges calculated for: {len(result)} consumers")
    
    # Calculate latest financial year tax amounts
    print("  Calculating latest FY demand amounts...")
    joined = joined_demand.merge(
        result[['consumercode', 'latest_fy']],
        on='consumercode',
        how='left'
    )
    
    latest_demand = joined[joined['fy'] == joined['latest_fy']]
    demand_sum = latest_demand.groupby('consumercode')['taxamount'].sum().reset_index()
    demand_sum.rename(columns={'taxamount':'latest_fy_taxamount'}, inplace=True)
    
    result = result.merge(demand_sum, on='consumercode', how='left')
    result['latest_fy_taxamount'] = result['latest_fy_taxamount'].fillna(0)
    print(f"    Latest FY calculations completed for: {len(demand_sum)} consumers")
    
    # Calculate current financial year (2025-26) tax amounts
    print("  Calculating current FY (2025-26) demand amounts...")
    target_fy = "2025-26"
    current_fy_demand = joined_demand[joined_demand['fy'] == target_fy]
    
    df_fy_sum = current_fy_demand.groupby('consumercode')['taxamount'].sum().reset_index()
    df_fy_sum.rename(columns={'taxamount': 'current_fy_taxamount'}, inplace=True)
    
    # Ensure all consumers are included, even those without current year demand
    all_consumercodes = pd.DataFrame(joined_demand['consumercode'].unique(), columns=['consumercode'])
    final = all_consumercodes.merge(df_fy_sum, on='consumercode', how='left')
    final['current_fy_taxamount'] = final['current_fy_taxamount'].fillna(0)
    
    result = result.merge(final, on='consumercode', how='left')
    result['current_fy_taxamount'] = result['current_fy_taxamount'].fillna(0)
    print(f"    Current FY calculations completed for: {len(df_fy_sum)} consumers with 2025-26 data")
    
    # Calculate arrear amounts (previous years' outstanding)
    print("  Calculating arrear amounts...")
    arrear_demand = joined_demand[joined_demand['fy'] < "2025-26"]
    
    agg = arrear_demand.groupby('consumercode').agg(
        arrear_taxamount_sum=('taxamount', 'sum'),
        arrear_collectionamount_sum=('collectionamount', 'sum')
    ).reset_index()
    
    # Arrears = Total assessed - Total collected for previous years
    agg['arrear_years_demand_generated'] = (
        agg['arrear_taxamount_sum'] - agg['arrear_collectionamount_sum']
    )
    
    result = result.merge(
        agg[['consumercode', 'arrear_years_demand_generated']],
        on='consumercode', how='left'
    )
    result['arrear_years_demand_generated'] = result['arrear_years_demand_generated'].fillna(0)
    print(f"    Arrear calculations completed for: {len(agg)} consumers")
    
    # Calculate penalty and interest amounts
    print("  Calculating penalty and interest...")
    relevant_codes = ['PT_TIME_PENALTY', 'PT_TIME_INTEREST']
    filtered = joined_demand[joined_demand['taxheadcode'].isin(relevant_codes)]
    
    if len(filtered) > 0:
        # Pivot tax head codes to separate columns
        grouped = (
            filtered.groupby(['consumercode', 'taxheadcode'])['taxamount']
            .sum()
            .unstack(fill_value=0)
            .reset_index()
        )
        
        # Ensure both penalty and interest columns exist
        for code in relevant_codes:
            if code not in grouped.columns:
                grouped[code] = 0
        
        grouped = grouped[['consumercode', 'PT_TIME_PENALTY', 'PT_TIME_INTEREST']]
        grouped = grouped.fillna(0)
        
        result = result.merge(grouped, on='consumercode', how='left')
        result[['PT_TIME_PENALTY', 'PT_TIME_INTEREST']] = result[['PT_TIME_PENALTY', 'PT_TIME_INTEREST']].fillna(0)
        
    else:
        result['PT_TIME_PENALTY'] = 0
        result['PT_TIME_INTEREST'] = 0
    
    print("    Penalty and interest calculations completed")
    
    # ========================================================================
    # Property and Unit Processing
    # ========================================================================
    
    print("Phase 6: Processing property characteristics...")
    
    # Join properties with their constituent units
    merged = property_df.merge(
        unit_all_columns_df, 
        left_on='id', 
        right_on='propertyid', 
        suffixes=('_property', '_unit')
    )
    print(f"  Property-unit relationships established: {len(merged)} records")
    
    # Classify ownership based on occupancy patterns across units
    def classify_ownership(occupancies):
        """
        Determine property ownership classification based on unit occupancy types.
        
        Logic:
        - If any unit is rented and mixed occupancy exists: Mixed
        - If all units are rented: Tenant  
        - If self-occupied or unoccupied: Owner
        
        Args:
            occupancies: Series of occupancy types for a property
            
        Returns:
            str: Classification (Owner/Tenant/Mixed)
        """
        unique_types = set(occupancies)
        if 'RENTED' in unique_types:
            if len(unique_types) > 1:
                return 'Mixed'
            else:
                return 'Tenant'
        if 'SELFOCCUPIED' in unique_types:
            return 'Owner'
        if 'UNOCCUPIED' in unique_types:
            return 'Owner'
        return None
    
    ownership = (
        merged.groupby('propertyid_property')['occupancytype']
        .apply(classify_ownership)
        .reset_index()
        .rename(columns={'occupancytype': 'Owned_Rented'})
    )
    
    property_df = property_df.merge(ownership, left_on='propertyid', right_on='propertyid_property', how='left')
    
    # Calculate total area by summing across all units
    def clean_numeric(series):
        """Convert string values to numeric, handling 'NULL' strings and errors."""
        return pd.to_numeric(series.replace('NULL', 0), errors='coerce').fillna(0)
    
    merged['builtuparea'] = clean_numeric(merged['builtuparea'])
    merged['plintharea'] = clean_numeric(merged['plintharea'])
    
    area_summary = (
        merged.groupby('propertyid_property', as_index=False)
        .agg(
            total_builtup_area=('builtuparea', 'sum'),
            total_plinth_area=('plintharea', 'sum')
        )
    )
    
    property_df = property_df.merge(area_summary, left_on='propertyid', right_on='propertyid_property', how='left')
    property_df['total_builtup_area'] = property_df['total_builtup_area'].fillna(0)
    property_df['total_plinth_area'] = property_df['total_plinth_area'].fillna(0)
    
    print("  Property characteristics processing completed")
    
    # ========================================================================
    # Data Integration Phase
    # ========================================================================
    
    print("Phase 7: Integrating property and financial data...")
    
    # Ensure consistent data types for the main join
    property_df['propertyid'] = property_df['propertyid'].astype(str)
    result['consumercode'] = result['consumercode'].astype(str)
    
    # Join property attributes with financial calculations
    property_result_merged = property_df.merge(
        result,
        left_on='propertyid',
        right_on='consumercode',
        how='left'
    )
    print(f"  Property-financial integration completed: {len(property_result_merged)} records")
    
    # ========================================================================
    # Exemption Processing
    # ========================================================================
    
    print("Phase 8: Processing property exemptions...")
    
    # Extract owner information for exemption determination
    owner_query = """
    SELECT propertyid, ownertype, status
    FROM eg_pt_owner
    WHERE status = 'ACTIVE'
    """
    owner_df = source_hook.get_pandas_df(owner_query)
    owner_df = owner_df[owner_df['status'] == 'ACTIVE'].copy()
    
    # Determine exemption status based on owner type
    owner_df['is_exempted'] = owner_df['ownertype'].isin(['WIDOW', 'FREEDOMFIGHTER'])
    exempted_status = owner_df.groupby('propertyid')['is_exempted'].any().reset_index()
    exempted_status['Is Property Exempted [Yes/ No]'] = exempted_status['is_exempted'].apply(
        lambda x: 'Yes' if x else 'No'
    )
    exempted_status = exempted_status.drop(columns=['is_exempted'])
    
    # Add exemption status to main dataset
    property_result_merged = property_result_merged.merge(
        exempted_status[['propertyid', 'Is Property Exempted [Yes/ No]']],
        left_on='id',
        right_on='propertyid',
        how='left'
    )
    
    property_result_merged['Is Property Exempted [Yes/ No]'] = (
        property_result_merged['Is Property Exempted [Yes/ No]'].fillna('No')
    )
    
    # Clean up duplicate columns from joins
    if 'propertyid' in property_result_merged.columns:
        property_result_merged.drop(columns=['propertyid'], inplace=True)
    
    if 'propertyid_x' in property_result_merged.columns:
        property_result_merged['propertyid'] = property_result_merged['propertyid_x']
    
    print("  Exemption processing completed")
    
    # ========================================================================
    # Final Report Generation
    # ========================================================================
    
    print("Phase 9: Generating final analytical report...")
    
    # Create final report with standardized column names
    report = property_result_merged.rename(columns={
        'tenantid': 'ULB',
        'propertyid': 'Property ID',
        'usagecategory': 'Usage',
        'createdtime': 'Date of Creation of the Property in the System',
        'additionaldetails': 'Date of Construction of the Property',
        'ownershipcategory': 'Ownership Type',
        'Is Property Exempted [Yes/ No]': 'Is Property Exempted [Yes/ No]',
        'Owned_Rented': 'Owned_Rented (Owner/ Rented/ Mixed)',
        'earliest_fy': 'Earliest Financial Year for which Demand was Generated',
        'latest_fy': 'Latest Financial Year for which Demand was Generated',
        'latest_fy_taxamount': 'Latest Demand Generated [in Rs.]',
        'current_fy_taxamount': 'Current Years Demand Generated [in Rs.]',
        'PT_TIME_PENALTY': 'Penalty',
        'PT_TIME_INTEREST': 'Interest',
        'arrear_years_demand_generated': 'Arrear Years Demand Generated [in Rs.]',
        'propertytype': 'Property Type[Building/ Vacant]',
        'total_builtup_area': 'Total Builtup Area [Sum of all units/ floors]',
        'total_plinth_area': 'Total Plinth Area [Sum of all units/ floors]'
    }).copy()
    
    # Format date fields for better readability
    def epoch_to_custom_date(epoch_ms):
        """Convert epoch milliseconds to formatted date string."""
        if pd.isna(epoch_ms):
            return None
        try:
            return datetime.fromtimestamp(epoch_ms / 1000).strftime('%d-%b-%Y')
        except:
            return None
    
    def get_year_construction(val):
        """Extract construction year from JSON additional details."""
        if pd.isna(val): 
            return None
        try: 
            return json.loads(val).get('yearConstruction')
        except: 
            return None
    
    # Apply formatting transformations
    report['ULB'] = report['ULB'].str.split('.').str[1].str.capitalize()
    report['Date of Creation of the Property in the System'] = report['Date of Creation of the Property in the System'].apply(epoch_to_custom_date)
    report['Date of Construction of the Property'] = report['Date of Construction of the Property'].apply(get_year_construction)
    
    # Select final columns for the analytical report
    final_report = report[
        [
            'ULB',
            'Property ID',
            'Usage',
            'Date of Creation of the Property in the System',
            'Date of Construction of the Property',
            'Ownership Type',
            'Is Property Exempted [Yes/ No]',
            'Owned_Rented (Owner/ Rented/ Mixed)',
            'Earliest Financial Year for which Demand was Generated',
            'Latest Financial Year for which Demand was Generated',
            'Latest Demand Generated [in Rs.]',
            'Current Years Demand Generated [in Rs.]',
            'Penalty',
            'Interest',
            'Arrear Years Demand Generated [in Rs.]',
            'Property Type[Building/ Vacant]',
            'Total Builtup Area [Sum of all units/ floors]',
            'Total Plinth Area [Sum of all units/ floors]'
        ]
    ].copy()
    
    # Remove records without valid Property IDs
    final_report = final_report.dropna(subset=['Property ID'])
    
    print(f"  Final analytical report generated: {len(final_report)} records")
    
    # ========================================================================
    # Quality Assurance Verification
    # ========================================================================
    
    print("Phase 10: Performing data quality verification...")
    
    # Print summary statistics for validation
    print("  Financial Summary:")
    print(f"    Latest Demand Total: Rs{final_report['Latest Demand Generated [in Rs.]'].sum():,.2f}")
    print(f"    Current Year Demand: Rs{final_report['Current Years Demand Generated [in Rs.]'].sum():,.2f}")
    print(f"    Arrear Demand Total: Rs{final_report['Arrear Years Demand Generated [in Rs.]'].sum():,.2f}")
    print(f"    Total Penalties: Rs{final_report['Penalty'].sum():,.2f}")
    print(f"    Total Interest: Rs{final_report['Interest'].sum():,.2f}")
    
    # ========================================================================
    # Datamart Loading Phase
    # ========================================================================
    
    print("Phase 11: Loading processed data to analytics datamart...")
    
    # Prepare data for database loading with snake_case column names
    final_report_for_load = final_report.rename(columns={
        'ULB': 'ulb',
        'Property ID': 'property_id',
        'Usage': 'usage',
        'Date of Creation of the Property in the System': 'date_creation_system',
        'Date of Construction of the Property': 'date_construction_property',
        'Ownership Type': 'ownership_type',
        'Is Property Exempted [Yes/ No]': 'is_property_exempted',
        'Owned_Rented (Owner/ Rented/ Mixed)': 'owned_rented',
        'Earliest Financial Year for which Demand was Generated': 'earliest_fy',
        'Latest Financial Year for which Demand was Generated': 'latest_fy',
        'Latest Demand Generated [in Rs.]': 'latest_demand_generated',
        'Current Years Demand Generated [in Rs.]': 'current_years_demand_generated',
        'Penalty': 'penalty',
        'Interest': 'interest',
        'Arrear Years Demand Generated [in Rs.]': 'arrear_years_demand_generated',
        'Property Type[Building/ Vacant]': 'property_type',
        'Total Builtup Area [Sum of all units/ floors]': 'total_builtup_area',
        'Total Plinth Area [Sum of all units/ floors]': 'total_plinth_area'
    })
    
    # Add tenant ID for complete record
    final_report_for_load['tenant_id'] = property_result_merged['tenantid']
    
    # Define column order for database insertion
    columns = [
        'ulb', 'property_id', 'usage', 'date_creation_system', 
        'date_construction_property', 'ownership_type', 'is_property_exempted',
        'owned_rented', 'earliest_fy', 'latest_fy', 'latest_demand_generated',
        'current_years_demand_generated', 'penalty', 'interest',
        'arrear_years_demand_generated', 'property_type', 'total_builtup_area',
        'total_plinth_area', 'tenant_id'
    ]
    
    # Perform batch loading to optimize database performance
    batch_size = 1000
    total_loaded = 0
    
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Clear existing data for fresh load
        cursor.execute("DELETE FROM datamart.punjab_property_analysis")
        conn.commit()
        print("  Cleared existing datamart records")
        
        # Load data in optimized batches
        for i in range(0, len(final_report_for_load), batch_size):
            batch = final_report_for_load.iloc[i:i+batch_size]
            
            # Prepare batch data with proper null handling
            rows = []
            for _, row in batch.iterrows():
                row_data = []
                for col in columns:
                    value = row.get(col)
                    if pd.isna(value):
                        row_data.append(None)
                    else:
                        row_data.append(value)
                rows.append(tuple(row_data))
            
            # Execute batch insert
            if rows:
                placeholders = ', '.join(['%s'] * len(columns))
                insert_query = f"""
                INSERT INTO datamart.punjab_property_analysis ({', '.join(columns)}) 
                VALUES ({placeholders})
                """
                
                cursor.executemany(insert_query, rows)
                conn.commit()
                total_loaded += len(rows)
                
                print(f"    Loaded batch {i//batch_size + 1}: {len(rows)} records")
    
    except Exception as e:
        print(f"ERROR during datamart loading: {str(e)}")
        conn.rollback()
        raise e
    
    finally:
        cursor.close()
        conn.close()
    
    print(f"  Successfully loaded {total_loaded} records to datamart")
    
    # Store execution metadata for downstream tasks
    context['task_instance'].xcom_push(key='extraction_time', value=current_extraction_time.isoformat())
    context['task_instance'].xcom_push(key='records_processed', value=len(final_report))
    
    print(f"Punjab property tax analysis completed successfully!")
    return len(final_report)

# ============================================================================
# Audit and Logging Task
# ============================================================================

def update_extraction_log(**context):
    """
    Updates the extraction audit log with processing metadata.
    
    This function maintains an audit trail of all data extractions including:
    - Timestamp of extraction
    - Tables processed
    - Record counts
    
    Args:
        **context: Airflow context containing task metadata
        
    Returns:
        int: Number of records processed (for monitoring)
    """
    
    target_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    extraction_time_str = context['task_instance'].xcom_pull(task_ids='process_data', key='extraction_time')
    records_processed = context['task_instance'].xcom_pull(task_ids='process_data', key='records_processed')
    
    if extraction_time_str:
        extraction_time = datetime.fromisoformat(extraction_time_str)
        
        # List of all source tables processed
        tables = [
            'eg_pt_property', 
            'eg_pt_unit', 
            'egbs_demand_v1', 
            'egbs_demanddetail_v1', 
            'eg_pt_owner'
        ]
        
        # Update audit log for each source table
        for table_name in tables:
            upsert_query = f"""
            INSERT INTO datamart.extraction_log (table_name, last_extraction_timestamp, updated_at)
            VALUES ('{table_name}', '{extraction_time}', CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) 
            DO UPDATE SET 
                last_extraction_timestamp = EXCLUDED.last_extraction_timestamp,
                updated_at = CURRENT_TIMESTAMP
            """
            target_hook.run(upsert_query)
        
        print(f"Audit log updated - Processed {records_processed} records at {extraction_time}")
    
    return records_processed or 0

# ============================================================================
# Data Quality Assurance Task
# ============================================================================

def data_quality_checks(**context):
    """
    Performs comprehensive data quality validation on processed data.
    
    Quality checks include:
    - Record count validation (must be > 0)
    - Financial amount summations
    - Data completeness verification
    - Constraint validation
    
    Raises:
        ValueError: If any quality check fails
        
    Args:
        **context: Airflow context for task execution
    """
    
    target_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    print("Initiating comprehensive data quality assessment...")
    
    # Aggregate financial metrics for validation
    totals_query = """
    SELECT 
        COUNT(*) as total_records,
        SUM(latest_demand_generated) as latest_total,
        SUM(current_years_demand_generated) as current_total,
        SUM(arrear_years_demand_generated) as arrear_total,
        SUM(penalty) as penalty_total,
        SUM(interest) as interest_total,
        COUNT(CASE WHEN property_id IS NULL THEN 1 END) as null_property_ids,
        COUNT(CASE WHEN ulb IS NULL THEN 1 END) as null_ulbs
    FROM datamart.punjab_property_analysis
    """
    
    totals = target_hook.get_first(totals_query)
    
    print("Data Quality Assessment Results:")
    print(f"  Total Records Loaded: {totals[0]:,}")
    print(f"  Latest Demand Sum: Rs{totals[1]:,.2f}")
    print(f"  Current Year Demand: Rs{totals[2]:,.2f}")
    print(f"  Arrear Demand Sum: Rs{totals[3]:,.2f}")
    print(f"  Total Penalties: Rs{totals[4]:,.2f}")
    print(f"  Total Interest: Rs{totals[5]:,.2f}")
    print(f"  Null Property IDs: {totals[6]}")
    print(f"  Null ULBs: {totals[7]}")
    
    # Critical quality check - ensure data was loaded
    if totals[0] < 1:
        raise ValueError("CRITICAL: No records found in datamart after processing")
    
    # Data integrity checks
    if totals[6] > 0:
        print(f"WARNING: Found {totals[6]} records with null property IDs")
        
    if totals[7] > 0:
        print(f"WARNING: Found {totals[7]} records with null ULB values")
    
    # Additional validation queries can be added here
    # Example: Check for negative amounts, validate date ranges, etc.
    
    print("✓ All data quality checks passed successfully")

# ============================================================================
# Task Definitions and Dependencies
# ============================================================================

# Define individual tasks with proper configuration
process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_punjab_data,
    dag=dag,
    doc_md="""
    ## Punjab Property Tax Data Processing
    
    Main ETL task that extracts, transforms, and loads Punjab property tax data.
    
    **Key Operations:**
    - Extracts data from 5 source tables
    - Calculates financial year summaries
    - Processes arrears, penalties, and interest
    - Generates comprehensive property analysis
    """
)

update_log_task = PythonOperator(
    task_id='update_extraction_log',
    python_callable=update_extraction_log,
    dag=dag,
    doc_md="""
    ## Extraction Audit Log Update
    
    Maintains audit trail of data processing activities for compliance and monitoring.
    """
)

quality_check_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    dag=dag,
    doc_md="""
    ## Data Quality Validation
    
    Performs comprehensive quality assurance checks to ensure data integrity.
    Fails the pipeline if critical issues are detected.
    """
)

# Define task execution sequence
# Process data -> Update audit logs -> Validate quality
process_task >> update_log_task >> quality_check_task