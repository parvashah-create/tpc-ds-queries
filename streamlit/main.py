import streamlit as st
from Functions.queries import query_22, query_19, query_17, query_15, query_26

def main():
    st.title("TPC-DS Queries")
    st.subheader("1. For each product name, brand, class, category, calculate the average quantity on hand. Rollup data by product name, brand, class and category.")
    dms = st.slider('Select the date month sequence', 0, 2389, 1000)
    st.write("Date Month Sequence: ", dms)
    if st.button("Execute"):
        result = query_22(dms)
        st.write(result)



    # Your code here
    st.subheader("""2. Get the top revenue generating products bought by out of zip code customers for a given year, month and manager.""")
    month = st.slider('Select the month', 1, 12, 11)
    year =  st.slider('Select year',1900,2100,1998)
    manager_id = st.slider('Select manager_id',1,100,8)
    
    if st.button('Execute',key="query_18"):
        result = query_19(month, year, manager_id)
        st.write(result)
    


    st.subheader("3. Analyze, for each state, all items that were sold in stores in a particular quarter and returned in the next three quarters and then re-purchased by the customer through the catalog channel in the three following quarters.")
    year = st.slider('select year',1900,2099,2001 , key="query_17")
    if st.button('Execute',key="btn_query_17"):
        result = query_17(year)
        st.write(result)



    st.subheader("4. Get the total catalog sales for customers in selected geographical regions or who made large purchases for given year and quarter.")
    d_year = st.slider('Select year',1900,2100,1998,key="query_15")
    d_qoy = st.number_input('Pick qoy',1,4,1)
    if st.button('Execute',key="btn_query_15"):
        result = query_15(d_qoy,d_year)
        st.write(result)

        

    st.subheader("get the average quantity, list price, discount, sales price for promotional items sold through the catalog channel where the promotion was not offered by mail or in an event for given gender, marital status and educational status.")
    cd_gender = st.selectbox("Select gender",("M", "F"))
    cd_marital_status = st.selectbox("Select marital status",("S", "D", "W", "U", "M"))
    cd_education_status = st.selectbox("Select educational status",("Secondary", "Advanced Degree", "2 yr Degree", "4 yr Degree", "Unknown", "Primary", "College"))
    d_year = st.slider('Select year',1900,2100,1998,key="query_26")
    if st.button('Execute',key="btn_query_26"):
        result = query_26(cd_gender,cd_marital_status,cd_education_status,d_year)
        st.write(result)
    
if __name__ == "__main__":
    main()
