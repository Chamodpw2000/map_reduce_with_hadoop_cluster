%%writefile dashboard.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.title("🌦 Weather Accident Analysis Dashboard")

st.write("Hadoop MapReduce Weather Analysis")

data = pd.read_csv("weather_output.txt", sep="\t", header=None)
data.columns = ["Weather", "Total Accidents", "Average Severity"]

data["Total Accidents"] = pd.to_numeric(data["Total Accidents"])
data["Average Severity"] = pd.to_numeric(data["Average Severity"])

st.subheader("Dataset")
st.dataframe(data)

st.subheader("Top 10 Weather Conditions")

top = data.sort_values(by="Total Accidents", ascending=False).head(10)

fig, ax = plt.subplots()
ax.barh(top["Weather"], top["Total Accidents"])
ax.set_xlabel("Accidents")
ax.set_title("Top Weather Conditions")
ax.invert_yaxis()

st.pyplot(fig)

st.subheader("Accidents vs Severity")

fig2, ax2 = plt.subplots()

ax2.scatter(data["Total Accidents"], data["Average Severity"])

ax2.set_xlabel("Total Accidents")
ax2.set_ylabel("Average Severity")

st.pyplot(fig2)
