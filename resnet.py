# Menginstal library yang dibutuhkan jika belum ada
%pip install tensorflow

# Impor semua library yang dibutuhkan
import os
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType
from tensorflow.keras.applications.resnet_v2 import ResNet50V2, preprocess_input, decode_predictions
from tensorflow.keras.preprocessing import image

# Tentukan path lengkap ke Volume Anda
volume_path = "/Volumes/workspace/myschema/myvolume/"
print(f"Membaca file dari: {volume_path}")

try:
    # Dapatkan daftar file menggunakan dbutils
    files = dbutils.fs.ls(volume_path)

    # Ambil path dari setiap file, pastikan itu adalah gambar, dan bersihkan prefix "dbfs:"
    image_paths = [
        file.path.replace("dbfs:", "") for file in files 
        if file.path.lower().endswith(('.png', '.jpg', '.jpeg'))
    ]

    if not image_paths:
        print("PERINGATAN: Tidak ada file gambar yang ditemukan di Volume. Pastikan path sudah benar dan file sudah diunggah.")
    else:
        # Buat DataFrame Spark dari daftar path
        df_image_paths = spark.createDataFrame(image_paths, StringType()).toDF("image_path")
        print(f"Ditemukan {df_image_paths.count()} file gambar yang akan diproses.")
        
        # Definisikan fungsi UDF untuk menerapkan model pada setiap file gambar
        @pandas_udf(StringType())
        def classify_images_udf(paths: pd.Series) -> pd.Series:
            # Muat model ResNet50V2 dengan bobot yang sudah dilatih dari ImageNet
            model = ResNet50V2(weights='imagenet')

            def predict_one_image(path):
                try:
                    
                    # Muat & ubah ukuran gambar ke 224x224
                    img = image.load_img(path, target_size=(224, 224))
                    
                    # Ubah gambar menjadi array numpy
                    x = image.img_to_array(img)
                    
                    # Tambahkan dimensi batch
                    x = np.expand_dims(x, axis=0)
                    
                    # Lakukan preprocessing sesuai standar model
                    x = preprocess_input(x)
                    
                    # Lakukan prediksi
                    preds = model.predict(x, verbose=0)
                    
                    # Dekode prediksi menjadi label yang bisa dibaca
                    decoded_preds = decode_predictions(preds, top=1)
                    label = decoded_preds[0][0][1]
                    return label
                except Exception as e:
                    return f"Error on path '{path}': {str(e)}"

            # Terapkan fungsi prediksi pada setiap path dalam batch
            return paths.apply(predict_one_image)

        # Terapkan UDF pada kolom 'image_path' untuk membuat kolom 'prediction' baru
        df_predictions = df_image_paths.withColumn("prediction", classify_images_udf(col("image_path")))

        # Tampilkan hasil akhir!
        print("Analisis selesai. Menampilkan hasil:")
        display(df_predictions)

except Exception as e:
    print(f"Terjadi kesalahan: {e}")
