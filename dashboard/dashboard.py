import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# ======================
# PAGE CONFIG
# ======================
st.set_page_config(
    page_title="E-commerce Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ======================
# LOAD DATA
# ======================
top_products = pd.read_csv("dashboard/top_products.csv")
top_users = pd.read_csv("dashboard/top_users.csv")

# ======================
# SIDEBAR
# ======================
st.sidebar.title("📊 Dashboard Controls")

top_n = st.sidebar.slider("Top N Records", 5, 20, 10)

top_products = top_products.head(top_n)
top_users = top_users.head(top_n)

# ======================
# HEADER
# ======================
st.title("📈 E-commerce Analytics Dashboard")
st.markdown("### Business Insights from Big Data Pipeline")

st.markdown("---")

# ======================
# KPI SECTION
# ======================
col1, col2, col3 = st.columns(3)

col1.metric(
    label="💰 Total Revenue (Top Products)",
    value=f"{top_products['revenue'].sum():,.2f}"
)

col2.metric(
    label="🛒 Total Purchases",
    value=f"{len(top_products):,}"
)

col3.metric(
    label="👤 Active Top Users",
    value=f"{len(top_users):,}"
)

st.markdown("---")

# ======================
# TABS SECTION
# ======================
tab1, tab2, tab3 = st.tabs(["📦 Products", "👤 Users", "📊 Insights"])

# ======================
# TAB 1: PRODUCTS
# ======================
with tab1:
    st.subheader("Top Products by Revenue")

    col1, col2 = st.columns([2, 1])

    with col1:
        fig, ax = plt.subplots()

        # Horizontal bar chart (cleaner)
        ax.barh(
            top_products['product_id'].astype(str),
            top_products['revenue']
        )

        ax.set_xlabel("Revenue")
        ax.set_ylabel("Product ID")
        ax.invert_yaxis()

        st.pyplot(fig)

    with col2:
        st.dataframe(
            top_products.style.format({"revenue": "{:,.2f}"})
        )

# ======================
# TAB 2: USERS
# ======================
with tab2:
    st.subheader("Top Users by Spending")

    col1, col2 = st.columns([2, 1])

    with col1:
        fig, ax = plt.subplots()

        ax.barh(
            top_users['user_id'].astype(str),
            top_users['total_spent']
        )

        ax.set_xlabel("Total Spending")
        ax.set_ylabel("User ID")
        ax.invert_yaxis()

        st.pyplot(fig)

    with col2:
        st.dataframe(
            top_users.style.format({"total_spent": "{:,.2f}"})
        )

# ======================
# TAB 3: INSIGHTS
# ======================
with tab3:
    st.subheader("Key Business Insights")

    st.markdown("""
    ### 📌 Observations:
    
    - Top products contribute significantly to total revenue  
    - A small number of users generate most of the revenue  
    - High-value customers can be targeted for retention strategies  

    ### 📈 Recommendations:
    
    - Focus marketing on top-performing products  
    - Provide offers to high-spending users  
    - Optimize inventory for popular categories  
    """)

# ======================
# FOOTER
# ======================
st.markdown("---")
st.caption("Built using PySpark + Databricks + Streamlit")