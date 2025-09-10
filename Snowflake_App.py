import streamlit as st
import snowflake.connector
import subprocess
import sys

# Install requirements.txt if not already installed
try:
    import snowflake.connector
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])


st.set_page_config(page_title="Snowflake Metadata Comparison")
st.title("Snowflake Environment Configuration and Metadata Comparison")

# Select environment to configure
env = st.selectbox("Select Environment to Configure", options=["DEV", "QA"])

st.subheader(f"Enter credentials for {env} environment")

# Initialize session state for both configs
if "dev_config" not in st.session_state:
    st.session_state.dev_config = {
        'user': '',
        'password': '',
        'account': '',
        'warehouse': '',
        'database': '',
        'schema': ''
    }

if "qa_config" not in st.session_state:
    st.session_state.qa_config = {
        'user': '',
        'password': '',
        'account': '',
        'warehouse': '',
        'database': '',
        'schema': ''
    }

# Load current config based on selected environment
current_config = st.session_state.dev_config if env == "DEV" else st.session_state.qa_config

# Input fields (masked for password/account)
user = st.text_input(f"{env} User", value=current_config['user'])
password = st.text_input(f"{env} Password", type="password", value=current_config['password'])
account = st.text_input(f"{env} Account", type="password", value=current_config['account'])
warehouse = st.text_input(f"{env} Warehouse", value=current_config['warehouse'])
database = st.text_input(f"{env} Database", value=current_config['database'])
schema = st.text_input(f"{env} Schema", value=current_config['schema'])

# Update session state
if env == "DEV":
    st.session_state.dev_config = {
        'user': user,
        'password': password,
        'account': account,
        'warehouse': warehouse,
        'database': database,
        'schema': schema
    }
else:
    st.session_state.qa_config = {
        'user': user,
        'password': password,
        'account': account,
        'warehouse': warehouse,
        'database': database,
        'schema': schema
    }

# Reset buttons
col_reset_dev, col_reset_qa = st.columns(2)
with col_reset_dev:
    if st.button("Reset DEV Config"):
        st.session_state.dev_config = {
            'user': '',
            'password': '',
            'account': '',
            'warehouse': '',
            'database': '',
            'schema': ''
        }
with col_reset_qa:
    if st.button("Reset QA Config"):
        st.session_state.qa_config = {
            'user': '',
            'password': '',
            'account': '',
            'warehouse': '',
            'database': '',
            'schema': ''
        }

# Display both configs
st.subheader("Stored Configurations")
col1, col2 = st.columns(2)
with col1:
    st.markdown("### DEV Config")
    st.json(st.session_state.dev_config)
with col2:
    st.markdown("### QA Config")
    st.json(st.session_state.qa_config)

# Metadata fetch function
def get_metadata(config):
    conn = snowflake.connector.connect(**config)
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW TABLES")
        tables = {row[1] for row in cursor.fetchall()}

        cursor.execute("SHOW VIEWS")
        views = {row[1] for row in cursor.fetchall()}

        cursor.execute(f"""
            SELECT table_name, column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = '{config['schema']}'
        """)
        columns = {}
        for table_name, column_name, data_type, char_len in cursor.fetchall():
            if table_name not in columns:
                columns[table_name] = []
            columns[table_name].append({
                'column': column_name,
                'type': data_type,
                'length': char_len
            })

        return tables, views, columns
    finally:
        cursor.close()
        conn.close()

# Compare metadata
st.subheader("Compare Metadata Between DEV and QA")
if st.button("Compare Metadata"):
    # If QA config is empty, copy from DEV
    if all(not v for v in st.session_state.qa_config.values()):
        st.session_state.qa_config = st.session_state.dev_config.copy()
        st.info("QA config was empty. Copied values from DEV.")

    progress = st.progress(0)
    try:
        progress.progress(10)
        tables_dev, views_dev, columns_dev = get_metadata(st.session_state.dev_config)
        progress.progress(40)
        tables_qa, views_qa, columns_qa = get_metadata(st.session_state.qa_config)
        progress.progress(70)

        table_diff = {
            'Only in DEV': sorted(tables_dev - tables_qa),
            'Only in QA': sorted(tables_qa - tables_dev)
        }

        view_diff = {
            'Only in DEV': sorted(views_dev - views_qa),
            'Only in QA': sorted(views_qa - views_dev)
        }

        column_diff = {}
        common_tables = tables_dev & tables_qa
        for table in common_tables:
            cols_dev = {col['column']: col for col in columns_dev.get(table, [])}
            cols_qa = {col['column']: col for col in columns_qa.get(table, [])}

            all_columns = set(cols_dev.keys()) | set(cols_qa.keys())
            diffs = []

            for col in all_columns:
                c_dev = cols_dev.get(col)
                c_qa = cols_qa.get(col)
                if c_dev != c_qa:
                    diffs.append({
                        'column': col,
                        'DEV': c_dev,
                        'QA': c_qa
                    })

            if diffs:
                column_diff[table] = diffs

        progress.progress(100)
        st.success("Metadata comparison completed!")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Table Differences")
            st.json(table_diff)
            st.subheader("View Differences")
            st.json(view_diff)
        with col2:
            st.subheader("Column Differences")
            for table, diffs in column_diff.items():
                st.markdown(f"**Table: {table}**")
                st.json(diffs)

    except Exception as e:
        st.error(f"Error comparing metadata: {e}")
        progress.empty()



# Add LinkedIn profile link at the bottom
st.markdown('---')
st.markdown('[Connect with me on LinkedIn](https://www.linkedin.com/in/mahammadjabiulla/)')
