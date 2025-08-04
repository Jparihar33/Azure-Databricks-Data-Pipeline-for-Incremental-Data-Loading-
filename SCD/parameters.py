# Databricks notebook source
datasets=[
    {
    'file_name':'orders'
    },
    {
    'file_name','customers'
    },
    {
    'file_name','products'
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("Output_dataset",datasets)