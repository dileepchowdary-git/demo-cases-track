import pandas as pd
from datetime import datetime
from clickhouse_connect import get_client
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import ssl
import urllib
import os
import certifi
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# ---------------- CONFIG ----------------
# Use environment variables for sensitive data
try:
    CLICK_PARAMS = {
        'host': os.getenv('CLICKHOUSE_HOST'),
        'port': int(os.getenv('CLICKHOUSE_PORT')),
        'username': os.getenv('CLICKHOUSE_USER'),
        'password': os.getenv('CLICKHOUSE_PASSWORD'),
        'database': os.getenv('CLICKHOUSE_DB')
    }
    RECIPIENT_EMAILS = os.getenv('RECIPIENT_EMAILS').split(',')
    SENDER_EMAIL = os.getenv('SENDER_EMAIL')
    SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
    
    # Simple check to ensure all critical variables are present
    if not all([CLICK_PARAMS['host'], CLICK_PARAMS['username'], CLICK_PARAMS['password'], SENDER_EMAIL, SENDGRID_API_KEY, RECIPIENT_EMAILS]):
        raise ValueError("One or more required environment variables are not set.")

except (ValueError, TypeError) as e:
    print(f"Error: {e}")
    print("Please make sure all required environment variables are set in your .env file.")
    exit(1)

# ---------------- SQL QUERIES ----------------
QUERY = """
WITH ranked_studies AS (
    SELECT 
        s.id AS study_id,
        s.client_fk,
        sd.is_demo,
        row_number() OVER (
            PARTITION BY s.client_fk, sd.is_demo
            ORDER BY s.created_at
        ) AS rank_within_type
    FROM Studies s
    JOIN StudyDetails sd ON s.id = sd.study_fk
),
latest_status AS (
    SELECT 
        ss.study_fk,
        ss.status,
        row_number() OVER (
            PARTITION BY ss.study_fk
            ORDER BY ss.created_at DESC
        ) AS rn
    FROM StudyStatuses ss
),
merged_parent AS (
    SELECT 
        s.id AS study_id,
        s.parent_fk,
        ps.status AS parent_status,
        ctm_parent.tat_min AS parent_tat_min
    FROM Studies s
    LEFT JOIN Studies ps ON s.parent_fk = ps.id
    LEFT JOIN metrics.client_tat_metrics ctm_parent ON ps.id = ctm_parent.study_id
    WHERE s.status = 'MERGED'
),
correct_rad AS (
    SELECT
        r.study_fk,
        r.by_user_fk AS rad_fk
    FROM StudyStatuses r
    INNER JOIN (
        SELECT study_fk, MIN(created_at) AS first_completed_time
        FROM StudyStatuses
        WHERE status = 'COMPLETED'
        GROUP BY study_fk
    ) AS f ON r.study_fk = f.study_fk
    WHERE r.status = 'REPORTED' AND r.created_at <= f.first_completed_time
),
preread_agent AS (
    SELECT study_fk, status, iqca_fk
    FROM StudyIqcs
    WHERE status = 'REPORTABLE'
),
studies_with_qc AS (
    SELECT DISTINCT study_fk
    FROM StudyQcs
)
SELECT 
    c.client_name AS Client_Name,
    s.created_at AS Study_Created_Time,
    sd.created_at AS Activated_Time,
    1 AS Activated_DemoCases,
    -- This column is no longer used for active case logic, but kept for summary stats
    CASE WHEN s.status NOT IN ('COMPLETED','DELETED') THEN 1 ELSE 0 END AS Active_DemoCases,
    CASE WHEN s.status = 'COMPLETED' THEN 1 ELSE 0 END AS Completed_DemoCases,
    -- THIS IS THE CRITICAL COLUMN FOR DETERMINING THE TRUE STATUS
    CASE
        WHEN s.status = 'MERGED' THEN 
            CASE
                WHEN r.study_fk IS NOT NULL AND r.status = 'COMPLETED' THEN 'Rework Completed'
                WHEN mp.parent_status = 'COMPLETED' THEN 'Completed'
                WHEN mp.parent_status NOT IN ('COMPLETED','DELETED') THEN 'Pending'
                ELSE mp.parent_status
            END
        ELSE
            CASE
                WHEN  r.study_fk IS NOT NULL AND r.status = 'COMPLETED' THEN 'Rework Completed'
                WHEN s.status = 'COMPLETED' THEN 'Completed'
                WHEN s.status NOT IN ('COMPLETED','DELETED') THEN 'Pending'
                ELSE s.status
            END
    END AS Final_Status,
    CASE
        WHEN s.status = 'MERGED' AND cr.rad_fk IN (2231, 1506, 1505, 2318, 1504, 2484, 2715, 2785) THEN 'HIL'
        WHEN s.status = 'MERGED' AND cr.rad_fk NOT IN (2231, 1506, 1505, 2318, 1504, 2484, 2715, 2785) THEN 'Radiologist'
        WHEN ls.status IN ('IQC_REVIEW','IQC_COMPLETED') THEN 'Preread'
        WHEN cr.rad_fk IN (2231, 1506, 1505, 2318, 1504, 2484, 2715, 2785) THEN 'HIL'
        WHEN cr.rad_fk NOT IN (2231, 1506, 1505, 2318, 1504, 2484, 2715, 2785) THEN 'Radiologist'
        ELSE NULL
    END AS Current_Bucket,
    concat('https://admin.5cnetwork.com/cases/', toString(sd.study_fk)) AS Study_Link,
    tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\\\', '')), 'list'))[1] AS modality,
    CASE WHEN s.status = 'MERGED' THEN mp.parent_tat_min ELSE ctm.tat_min END AS tat_min,
    CASE
        WHEN modality = 'XRAY' AND (CASE WHEN s.status = 'MERGED' THEN mp.parent_tat_min ELSE ctm.tat_min END) <= 60 THEN 'Green'
        WHEN modality = 'CT' AND (CASE WHEN s.status = 'MERGED' THEN mp.parent_tat_min ELSE ctm.tat_min END) <= 120 THEN 'Green'
        WHEN modality = 'MRI' AND (CASE WHEN s.status = 'MERGED' THEN mp.parent_tat_min ELSE ctm.tat_min END) <= 180 THEN 'Green'
        WHEN modality = 'NM' AND (CASE WHEN s.status = 'MERGED' THEN mp.parent_tat_min ELSE ctm.tat_min END) <= 1440 THEN 'Green'
        ELSE 'Red'
    END AS TAT_Flag,
    cd.client_source as Clinet_source,
    cg.assigned_to as assigned_to,
    pods.pod_name as pod_name,
    CASE
        WHEN rs.is_demo = 1 THEN concat('Demo Case #', toString(rs.rank_within_type))
    END AS Case_Tag,
    CASE
        WHEN cr.rad_fk IN (2231,1506,1505,2318,1504,2484,2715,2785) THEN 'Keerthana R'
        WHEN ls.status IN ('IQC_REVIEW','IQC_COMPLETED') AND pa.iqca_fk IS NOT NULL AND 
             pa.iqca_fk NOT IN (SELECT DISTINCT qc_fk FROM QcRoster) THEN 'Bhuvaneswaran'
        WHEN ls.status IN ('IQC_REVIEW','IQC_COMPLETED') AND pa.iqca_fk IS NOT NULL AND 
             pa.iqca_fk IN (SELECT DISTINCT qc_fk FROM QcRoster) THEN 'Santosh Kumar'
        WHEN cr.rad_fk NOT IN (1505,2484,2715,2785,2231,2765) THEN 'Ruksana'
        ELSE NULL
    END AS category_manager

FROM Studies AS s
INNER JOIN StudyDetails AS sd ON sd.study_fk = s.id
LEFT JOIN Clients AS c ON s.client_fk = c.id
LEFT JOIN Reworks AS r ON sd.study_fk = r.study_fk
LEFT JOIN metrics.client_tat_metrics AS ctm ON sd.study_fk = ctm.study_id
LEFT JOIN ClientDetails AS cd ON s.client_fk = cd.client_fk
LEFT JOIN metrics.client_group AS cg ON s.client_fk = cg.client_fk
LEFT JOIN market_analysis_tables.pods AS pods ON cg.pod_id = pods.id
LEFT JOIN ranked_studies AS rs ON sd.study_fk = rs.study_id
LEFT JOIN latest_status AS ls ON sd.study_fk = ls.study_fk AND ls.rn = 1
LEFT JOIN merged_parent AS mp ON s.id = mp.study_id
LEFT JOIN correct_rad AS cr ON sd.study_fk = cr.study_fk
LEFT JOIN preread_agent AS pa ON sd.study_fk = pa.study_fk
LEFT JOIN studies_with_qc AS swq ON sd.study_fk = swq.study_fk
WHERE sd.is_demo = 1
  AND sd.created_at BETWEEN now() - INTERVAL 20 DAY AND now()
ORDER BY Final_Status, sd.created_at ASC
"""

NON_DEMO_QUERY = """
WITH ranked_studies AS (
    SELECT 
        s.id AS study_id,
        s.client_fk,
        sd.is_demo,
        row_number() OVER (
            PARTITION BY s.client_fk
            ORDER BY s.created_at
        ) AS rank_within_type
    FROM Studies s
    JOIN StudyDetails sd ON s.id = sd.study_fk
),
demo_clients AS (
    SELECT DISTINCT
        s.client_fk
    FROM Studies AS s
    INNER JOIN StudyDetails AS sd ON sd.study_fk = s.id
    WHERE sd.is_demo = 1
      AND sd.created_at BETWEEN now() - INTERVAL 20 DAY AND now()
)
SELECT 
    c.client_name AS Client_Name,
    s.created_at AS Study_Created_Time,
    s.status AS Final_Status,
    tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\\\', '')), 'list'))[1] AS modality,
    ctm.tat_min AS tat_min,
    CASE
        WHEN modality = 'XRAY' AND ctm.tat_min <= 60 THEN 'Green'
        WHEN modality = 'CT' AND ctm.tat_min <= 120 THEN 'Green'
        WHEN modality = 'MRI' AND ctm.tat_min <= 180 THEN 'Green'
        WHEN modality = 'NM' AND ctm.tat_min <= 1440 THEN 'Green'
        ELSE 'Red'
    END AS TAT_Flag,
    concat('https://admin.5cnetwork.com/cases/', toString(s.id)) AS Study_Link,
    cg.assigned_to AS assigned_to,
    pods.pod_name AS pod_name,
    CASE
        WHEN rs.is_demo = 0 AND rs.rank_within_type <= 5 THEN
            CASE
                WHEN rs.rank_within_type = 1 THEN '1st Real Case'
                WHEN rs.rank_within_type = 2 THEN '2nd Real Case'
                WHEN rs.rank_within_type = 3 THEN '3rd Real Case'
                ELSE concat(toString(rs.rank_within_type), 'th Real Case')
            END
        ELSE concat(toString(rs.rank_within_type), 'th Real Case')
    END AS Tag
FROM Studies AS s
INNER JOIN StudyDetails AS sd ON sd.study_fk = s.id
LEFT JOIN Clients AS c ON s.client_fk = c.id
LEFT JOIN ranked_studies AS rs ON sd.study_fk = rs.study_id
LEFT JOIN metrics.client_tat_metrics AS ctm ON sd.study_fk = ctm.study_id
LEFT JOIN metrics.client_group AS cg ON s.client_fk = cg.client_fk
LEFT JOIN market_analysis_tables.pods AS pods ON cg.pod_id = pods.id
WHERE sd.is_demo = 0 
  AND s.created_at BETWEEN now() - INTERVAL 20 DAY AND now() -- New condition to filter by last 24 hours
  AND rs.rank_within_type <= 5
  AND s.client_fk IN (SELECT client_fk FROM demo_clients)
ORDER BY s.client_fk ASC, s.created_at ASC
"""


# ---------------- FUNCTIONS ----------------
def execute_query_and_send_email():
    """Run queries on ClickHouse, format results, and send email"""
    try:
        # Connect to ClickHouse using environment variables
        client = get_client(
            host=CLICK_PARAMS['host'],
            port=CLICK_PARAMS['port'],
            username=CLICK_PARAMS['username'],
            password=CLICK_PARAMS['password'],
            database=CLICK_PARAMS['database']
        )
        print("Connected to ClickHouse successfully")

        # Run demo cases query
        print("Executing demo cases query...")
        df_demo = client.query_df(QUERY)
        print(f"Demo query returned {len(df_demo)} rows")

        # Run non-demo cases query
        print("Executing non-demo cases query...")
        df_non_demo = client.query_df(NON_DEMO_QUERY)
        print(f"Non-demo query returned {len(df_non_demo)} rows")

        if df_demo.empty and df_non_demo.empty:
            print("No data found.")
            return

        # A case is active if its Final_Status is not 'Completed', 'Rework Completed', or 'DELETED'.
        if not df_demo.empty:
            active_demo_cases = df_demo[~df_demo['Final_Status'].isin(['Completed', 'Rework Completed', 'DELETED'])]
        else:
            active_demo_cases = pd.DataFrame()
        
        if not df_non_demo.empty:
            active_non_demo_cases = df_non_demo[~df_non_demo['Final_Status'].isin(['COMPLETED', 'DELETED'])]
        else:
            active_non_demo_cases = pd.DataFrame()
        
        # Check for TAT breach cases (completed demo cases that exceeded TAT)
        tat_breach_demo = df_demo[(df_demo['Final_Status'].isin(['Completed', 'Rework Completed'])) & (df_demo['TAT_Flag'] == 'Red')] if not df_demo.empty else pd.DataFrame()
        
        # Create a separate DataFrame for 'Rework Completed' cases
        rework_completed_demo = df_demo[df_demo['Final_Status'] == 'Rework Completed'] if not df_demo.empty else pd.DataFrame()

        # The email will ONLY be sent if there are active cases.
        if active_demo_cases.empty and active_non_demo_cases.empty:
            print("No active cases found. Email will not be sent.")
            return

        print(f"Found {len(active_demo_cases)} active demo cases, {len(active_non_demo_cases)} active non-demo cases")
        print(f"Found {len(tat_breach_demo)} TAT breach demo cases")
        print(f"Found {len(rework_completed_demo)} rework completed demo cases")

        # Convert to HTML tables
        print("Creating HTML table...")
        html_content = create_html_table(df_demo, df_non_demo, active_demo_cases, active_non_demo_cases, tat_breach_demo, rework_completed_demo)

        # Send email to multiple recipients
        print("Sending email...")
        send_html_email_sendgrid(html_content, RECIPIENT_EMAILS)
        print("Email sent successfully!")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
# CORRECTED FUNCTION SIGNATURE: Added rework_completed_demo argument
def create_html_table(df_demo: pd.DataFrame, df_non_demo: pd.DataFrame, active_demo_cases: pd.DataFrame, active_non_demo_cases: pd.DataFrame, tat_breach_demo: pd.DataFrame, rework_completed_demo: pd.DataFrame) -> str:
    """Convert DataFrames to a professional and compact HTML email with separate boxes and a clean table."""
    # Summary stats are calculated on ALL demo data from the query
    
    # Exclude 'DELETED' cases from the total count
    completed_and_active_df = df_demo[df_demo['Final_Status'] != 'DELETED']
    total_cases = len(completed_and_active_df)
    
    active_cases = len(active_demo_cases)
    
    completed_df = df_demo[df_demo['Final_Status'].isin(['Completed', 'Rework Completed'])]
    completed_cases = len(completed_df)
    
    if len(completed_df) > 0:
        within_tat_completed = completed_df[completed_df['TAT_Flag'] == 'Green']
        green_tat = len(within_tat_completed)
        red_tat = len(completed_df) - green_tat
    else:
        green_tat = 0
        red_tat = 0
    
    # Prepare data for HTML tables
    demo_table_data = active_demo_cases.copy()
    non_demo_table_data = active_non_demo_cases.copy()
    tat_breach_demo_data = tat_breach_demo.copy()
    rework_table_data = rework_completed_demo.copy()
    
    # Apply hyperlink formatting to all dataframes
    for df in [demo_table_data, non_demo_table_data, tat_breach_demo_data, rework_table_data]:
        if not df.empty and 'Study_Link' in df.columns:
            df['Client_Name'] = df.apply(
                lambda row: f'<a href="{row["Study_Link"]}" target="_blank">{row["Client_Name"]}</a>',
                axis=1
            )
            df.drop('Study_Link', axis=1, inplace=True)

    # --- HTML Generation ---
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Active Cases Report</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                margin: 0;
                padding: 15px;
                background-color: #f7f9fc;
                color: #333;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background-color: #ffffff;
                border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.08);
                overflow: hidden;
            }}
            .header {{
                background-color: #3f51b5;
                color: white;
                padding: 8px 20px;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 24px;
                font-weight: 600;
            }}
            .header p {{
                margin: 2px 0 0;
                font-size: 13px;
                opacity: 0.9;
            }}
            .content {{
                padding: 20px;
            }}
            .summary-grid {{
                display: flex;
                flex-wrap: wrap;
                justify-content: space-between;
                gap: 15px;
                margin-bottom: 15px;
            }}
            .summary-card {{
                flex: 1;
                min-width: 150px;
                background-color: #ffffff;
                border: 1px solid #e0e6ed;
                border-radius: 6px;
                padding: 15px;
                text-align: center;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }}
            .summary-card h3 {{
                font-size: 13px;
                color: #6c757d;
                margin: 0 0 5px 0;
                text-transform: uppercase;
                font-weight: 500;
            }}
            .summary-card .number {{
                font-size: 32px;
                font-weight: 700;
                color: #212529;
            }}
            .green-number {{ color: #28a745 !important; }}
            .red-number {{ color: #dc3545 !important; }}
            .orange-number {{ color: #fd7e14 !important; }}
            
            h2 {{
                font-size: 18px;
                margin-top: 25px;
                margin-bottom: 10px;
                color: #3f51b5;
                border-bottom: 2px solid #e0e6ed;
                padding-bottom: 5px;
            }}
            .table-container {{
                margin-top: 10px;
                overflow-x: auto;
                border: 1px solid #dee2e6;
                border-radius: 6px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 12px;
            }}
            th, td {{
                padding: 8px 10px;
                text-align: left;
                border-bottom: 1px solid #e9ecef;
            }}
            thead th {{
                background-color: #f8f9fa;
                font-weight: 600;
                color: #495057;
                font-size: 11px;
                text-transform: uppercase;
            }}
            tbody tr:nth-child(even) {{ background-color: #fdfdfd; }}
            tbody tr:hover {{ background-color: #f1f3f5; }}
            
            .tat-red {{
                background-color: #ffe6e8;
                color: #b32d3a;
                font-weight: bold;
                text-align: center;
            }}
            .tat-green {{
                background-color: #d4edda;
                color: #155724;
                font-weight: bold;
                text-align: center;
            }}
            .tat-pending {{
                background-color: #fff3cd;
                color: #856404;
                font-weight: bold;
                text-align: center;
            }}
            .footer {{
                padding: 15px;
                text-align: center;
                font-size: 12px;
                color: #6c757d;
                background-color: #f8f9fa;
                border-top: 1px solid #e9ecef;
            }}
            .status-badge {{
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 10px;
                font-weight: 600;
                display: inline-block;
            }}
            .status-completed {{ background-color: #d4edda; color: #155724; }}
            .status-pending {{ background-color: #fff3cd; color: #856404; }}
            .status-rework {{ background-color: #d6d8db; color: #343a40; }}
            .no-data {{
                text-align: center;
                padding: 20px;
                color: #6c757d;
                font-style: italic;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🩺 5C Network Cases Report</h1>
                <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
            </div>
            <div class="content">
                <h2>📊 Summary - All Cases (Last 10 Days)</h2>
                <div class="summary-grid">
                    <div class="summary-card">
                        <h3>TOTAL DEMO</h3>
                        <div class="number">{total_cases}</div>
                    </div>
                    <div class="summary-card">
                        <h3>ACTIVE DEMO</h3>
                        <div class="number orange-number">{active_cases}</div>
                    </div>
                    <div class="summary-card">
                        <h3>COMPLETED</h3>
                        <div class="number">{completed_cases}</div>
                    </div>
                    <div class="summary-card">
                        <h3>WITHIN TAT</h3>
                        <div class="number green-number">{green_tat}</div>
                    </div>
                    <div class="summary-card">
                        <h3>EXCEEDING TAT</h3>
                        <div class="number red-number">{red_tat}</div>
                    </div>
                </div>
                
                <h2>Active Demo Cases</h2>"""
    
    if not demo_table_data.empty:
        html += f"""
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Client Name</th>
                                <th>Activated</th>
                                <th>Status</th>
                                <th>Current Bucket</th>
                                <th>Modality</th>
                                <th>TAT (min)</th>
                                <th>Client Source</th>
                                <th>Assigned To</th>
                                <th>Pod Name</th>
                                <th>Case Tag</th>
                                <th>Category Manager</th>
                            </tr>
                        </thead>
                        <tbody>
                            {"".join([f'''<tr>
                                <td>{row["Client_Name"]}</td>
                                <td>{row["Activated_Time"].strftime("%b %d, %H:%M")}</td>
                                <td><span class="status-badge status-pending">{row["Final_Status"]}</span></td>
                                <td>{row["Current_Bucket"]}</td>
                                <td>{row["modality"]}</td>
                                <td class="tat-pending">Pending</td>
                                <td>{row["Clinet_source"]}</td>
                                <td>{row["assigned_to"]}</td>
                                <td>{row["pod_name"]}</td>
                                <td>{row["Case_Tag"]}</td>
                                <td>{row["category_manager"]}</td>
                            </tr>''' for _, row in demo_table_data.iterrows()])}
                        </tbody>
                    </table>
                </div>"""
    else:
        html += '<div class="no-data">No active demo cases found.</div>'
    
    html += '<h2>Active First 5 Real Cases</h2>'
    
    if not non_demo_table_data.empty:
        html += f"""
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Client Name</th>
                                <th>Created</th>
                                <th>Modality</th>
                                <th>Status</th>
                                <th>TAT (min)</th>
                                <th>Tag</th>
                                <th>Assigned To</th>
                                <th>Pod Name</th>
                            </tr>
                        </thead>
                        <tbody>
                            {"".join([f'''<tr>
                                <td>{row["Client_Name"]}</td>
                                <td>{row["Study_Created_Time"].strftime("%b %d, %H:%M")}</td>
                                <td>{row["modality"]}</td>
                                <td><span class="status-badge status-pending">{row["Final_Status"]}</span></td>
                                <td class="tat-pending">Pending</td>
                                <td>{row["Tag"]}</td>
                                <td>{row["assigned_to"]}</td>
                                <td>{row["pod_name"]}</td>
                            </tr>''' for _, row in non_demo_table_data.iterrows()])}
                        </tbody>
                    </table>
                </div>"""
    else:
        html += '<div class="no-data">No active real cases found.</div>'

    html += '<h2>🔧 Rework Completed Demo Cases</h2>'
    
    if not rework_table_data.empty:
        html += f"""
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Client Name</th>
                                <th>Activated</th>
                                <th>Status</th>
                                <th>Current Bucket</th>
                                <th>Modality</th>
                                <th>TAT (min)</th>
                                <th>Client Source</th>
                                <th>Assigned To</th>
                                <th>Pod Name</th>
                                <th>Case Tag</th>
                                <th>Category Manager</th>
                            </tr>
                        </thead>
                        <tbody>
                            {"".join([f'''<tr>
                                <td>{row["Client_Name"]}</td>
                                <td>{row["Activated_Time"].strftime("%b %d, %H:%M")}</td>
                                <td><span class="status-badge status-rework">{row["Final_Status"]}</span></td>
                                <td>{row["Current_Bucket"]}</td>
                                <td>{row["modality"]}</td>
                                <td class="{'tat-red' if row['TAT_Flag'] == 'Red' else 'tat-green'}">{row["tat_min"]}</td>
                                <td>{row["Clinet_source"]}</td>
                                <td>{row["assigned_to"]}</td>
                                <td>{row["pod_name"]}</td>
                                <td>{row["Case_Tag"]}</td>
                                <td>{row["category_manager"]}</td>
                            </tr>''' for _, row in rework_table_data.iterrows()])}
                        </tbody>
                    </table>
                </div>"""
    else:
        html += '<div class="no-data">No rework completed demo cases found.</div>'

    html += '<h2>⚠️ TAT Breach Demo Cases (Completed)</h2>'
    
    if not tat_breach_demo_data.empty:
        html += f"""
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Client Name</th>
                                <th>Activated</th>
                                <th>Status</th>
                                <th>Current Bucket</th>
                                <th>Modality</th>
                                <th>TAT (min)</th>
                                <th>Client Source</th>
                                <th>Assigned To</th>
                                <th>Pod Name</th>
                                <th>Case Tag</th>
                                <th>Category Manager</th>
                            </tr>
                        </thead>
                        <tbody>
                            {"".join([f'''<tr>
                                <td>{row["Client_Name"]}</td>
                                <td>{row["Activated_Time"].strftime("%b %d, %H:%M")}</td>
                                <td><span class="status-badge status-completed">{row["Final_Status"]}</span></td>
                                <td>{row["Current_Bucket"]}</td>
                                <td>{row["modality"]}</td>
                                <td class="tat-red">{row["tat_min"]}</td>
                                <td>{row["Clinet_source"]}</td>
                                <td>{row["assigned_to"]}</td>
                                <td>{row["pod_name"]}</td>
                                <td>{row["Case_Tag"]}</td>
                                <td>{row["category_manager"]}</td>
                            </tr>''' for _, row in tat_breach_demo_data.iterrows()])}
                        </tbody>
                    </table>
                </div>"""
    else:
        html += '<div class="no-data">No TAT breach demo cases found.</div>'

    html += f"""
            </div>
            <div class="footer">
                <p><strong>5C Network</strong> | Active Cases & TAT Breach Alert System</p>
                <p>This email is sent when there are active cases or TAT breaches requiring attention</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

def send_html_email_sendgrid(html_content: str, recipients: list):
    """Send HTML email with SendGrid to multiple recipients"""
    
    try:
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
        os.environ['SSL_CERT_FILE'] = certifi.where()

        ssl_context = ssl.create_default_context(cafile=certifi.where())
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl_context))
        urllib.request.install_opener(opener)

    except Exception as e:
        print(f"SSL context setup failed, falling back to unverified context: {e}")
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl_context))
        urllib.request.install_opener(opener)

    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=recipients,
        subject=f"🩺 5C Network Demo Cases Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        html_content=html_content
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"SendGrid Response: {response.status_code}")
        if response.status_code == 202:
            print("✅ Email sent successfully!")
        else:
            print(f"⚠️  Unexpected response code: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        raise e


# ---------------- MAIN ----------------
if __name__ == "__main__":
    execute_query_and_send_email()