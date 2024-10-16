# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Order! :cup_with_straw:")
st.write(
    """Orders that need to be filled!
    """
)

session = get_active_session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if my_dataframe:
    #Add a line to your new app the converts the dataframe to a data editor. 
    editable_df = st.data_editor(my_dataframe)
    
    submitted = st.button('Submit')
    
    if submitted:
        st.success('Someone clicked the button', icon = '👍')
        try:
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)
            og_dataset.merge(edited_dataset
                                 , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                                 , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                                )
            st.write(og_dataset)
            st.success("Orders Updated!", icon='👍')
        except:
            st.write("Something went wrong")
else:
    st.success("There is no pending orders right now!", icon='👍')
