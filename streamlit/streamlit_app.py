import streamlit as st

st.write("""
# Observabilidade
Um aplicativo para acompanhar pipelines
         """)

date = st.date_input("Calendário")

file = st.file_uploader("Escolha um arquivo")

color = st.color_picker("Escolha uma cor")

slider = st.slider('Filtro de faixa de valores', -1000, 1000)
