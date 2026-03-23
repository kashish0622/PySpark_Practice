{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyNwH2zdj5VduqbzMwNi5OE0",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/kashish0622/PySpark_Practice/blob/main/write_csv.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 1,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "X3oVu6MbGm5j",
        "outputId": "80e2fd69-e0fb-4c4a-9f47-cbf2a29dfe34"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Requirement already satisfied: pyspark in /usr/local/lib/python3.12/dist-packages (4.0.2)\n",
            "Requirement already satisfied: py4j<0.10.9.10,>=0.10.9.7 in /usr/local/lib/python3.12/dist-packages (from pyspark) (0.10.9.9)\n"
          ]
        }
      ],
      "source": [
        "%pip install pyspark\n"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql import SparkSession\n",
        "spark = SparkSession.builder.appName(\"CSV\").getOrCreate()\n",
        "from pyspark.sql.types import *\n",
        "schema = StructType([\n",
        "    StructField(\"id\", IntegerType(), True),\n",
        "    StructField(\"name\", StringType(), True),\n",
        "    StructField(\"product\", StringType(), True),\n",
        "    StructField(\"category\", StringType(), True),\n",
        "    StructField(\"quantity\", IntegerType(), True),\n",
        "    StructField(\"price\", IntegerType(), True),\n",
        "    StructField(\"city\", StringType(), True),\n",
        "])\n",
        "\n",
        "data = [\n",
        "    (101, \"Amit Sharma\", \"Laptop\", \"Electronics\", 1, 55000, \"Delhi\"),\n",
        "    (102, \"Priya Singh\", \"Mobile\", \"Electronics\", 2, 15000, \"Mumbai\"),\n",
        "    (103, \"Rahul Verma\", \"Shoes\", \"Fashion\", 3, 2000, \"Bangalore\"),\n",
        "    (104, \"Neha Gupta\", \"Watch\", \"Accessories\", 1, 5000, \"Delhi\"),\n",
        "    (105, \"Arjun Mehta\", \"Headphones\", \"Electronics\", 2, 3000, \"Pune\"),\n",
        "    (106, \"Sneha Reddy\", \"Bag\", \"Fashion\", 1, 2500, \"Hyderabad\"),\n",
        "    (107, \"Vikram Joshi\", \"Tablet\", \"Electronics\", 1, 20000, \"Chennai\"),\n",
        "    (108, \"Kavita Nair\" , \"Sunglasses\", \"Accessories\", 2, 1500, \"Kochi\")\n",
        "]\n",
        "\n",
        "df = spark.createDataFrame(data, schema)\n",
        "#df.show()\n",
        "\n",
        "df.write\\\n",
        "    .mode(\"overwrite\")\\\n",
        "    .option(\"header\", True)\\\n",
        "    .csv(\"output/csv_data\")\n",
        "\n",
        "df.printSchema()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "_uJiytOkGwse",
        "outputId": "5dc75b03-3aca-4ed1-e31b-63c6d172a09f"
      },
      "execution_count": 4,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "root\n",
            " |-- id: integer (nullable = true)\n",
            " |-- name: string (nullable = true)\n",
            " |-- product: string (nullable = true)\n",
            " |-- category: string (nullable = true)\n",
            " |-- quantity: integer (nullable = true)\n",
            " |-- price: integer (nullable = true)\n",
            " |-- city: string (nullable = true)\n",
            "\n"
          ]
        }
      ]
    }
  ]
}