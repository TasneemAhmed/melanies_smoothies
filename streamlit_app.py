# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
    """Chhose the fruits you want in custom Smoothie!
    """
)

order_name = st.text_input("The name of Your Smoothie")
st.write("The name Smoothie will be ", order_name)
time_to_insert_name = st.button('Submit name')

session = get_active_session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('fruit_name'))


ingredients_list = st.multiselect(
    "Choose up to 5 Ingredients",
    my_dataframe,
    max_selections = 5
)

if ingredients_list:

    ingredients_string = ''

    for fruit in ingredients_list:
        ingredients_string += fruit + ' '

    st.write(ingredients_string)

    time_to_insert = st.button('Submit order')
    if time_to_insert and ingredients_string:
        
        my_insert_stmt = """INSERT INTO smoothies.public.orders(ingredients, name_on_order)
                    VALUES ('""" + ingredients_string + "', '" + order_name + """')"""
        
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {order_name}!', icon="✅")
