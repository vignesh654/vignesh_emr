# Databricks notebook source
textFile = spark.read.text("/databricks-datasets/samples/docs/README.md")
textFile.count()