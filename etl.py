import dlt
from pyspark.sql.functions import *

# bronze layer
@dlt.table(
  name="marketing_bronze",
  comment="Data marketing mentah dari workspace.lakeschema.marketing"
)
def marketing_bronze():
    return (
      spark.readStream.table("workspace.lakeschema.marketing")
    )

# silver layer
@dlt.table(
  name="marketing_silver",
  comment="Prep data marketing"
)
def marketing_silver():
    return (
      dlt.readStream("marketing_bronze")
        .withColumnRenamed("y", "subscribed")  # Ganti nama 'y' jadi 'subscribed'
        .withColumn("subscribed", col("subscribed") == "yes") # Ubah "yes"/"no" jadi true/false
        .withColumn("has_housing_loan", col("housing") == "yes")
        .withColumn("has_personal_loan", col("loan") == "yes")
        .withColumn("has_defaulted", col("default") == "yes")
        .withColumn("job", lower(col("job"))) # Seragamkan 'job' jadi huruf kecil
        .withColumn("education", lower(col("education"))) # Seragamkan 'education'
        .select(
            "age",
            "job",
            "marital",
            "education",
            "balance",
            "duration",
            "campaign",
            "poutcome",
            "subscribed",
            "has_housing_loan",
            "has_personal_loan",
            "has_defaulted"
        )
    )

# gold layer 
@dlt.table(
  name="laporan_konversi_per_pekerjaan",
  comment="Tabel agregat: tingkat konversi ('subscribed') berdasarkan 'job'"
)
def laporan_konversi_per_pekerjaan():
    
    df = dlt.read("marketing_silver") 

    # Hitung total nasabah dan yang berlangganan, per pekerjaan
    df_agg = df.groupBy("job").agg(
        count("*").alias("total_nasabah"),
        sum(when(col("subscribed") == True, 1).otherwise(0)).alias("jumlah_berlangganan")
    )
    
    # Hitung persentase konversi
    df_report = df_agg.withColumn(
        "tingkat_konversi", 
        (col("jumlah_berlangganan") / col("total_nasabah")) * 100
    ).orderBy(desc("tingkat_konversi"))
    
    return df_report