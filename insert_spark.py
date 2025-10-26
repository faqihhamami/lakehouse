# 1 lokasi schema
ABLE_TO_APPEND = "workspace.lakeschema.marketing"

# 2. buat dummy data
# (age, job, marital, education, default, balance, housing, loan, contact, 
#  day, month, duration, campaign, pdays, previous, poutcome, y)
newData = [
    (100, 'student', 'single', 'unknown', 'no', 500, 'no', 'no', 'cellular', 
     25, 'oct', 300, 1, -1, 0, 'unknown', 'yes'),
    
    (101, 'retired', 'married', 'primary', 'no', 25000, 'yes', 'no', 'telephone', 
     25, 'oct', 120, 2, -1, 0, 'unknown', 'no')
]

# 3. tentukan kolom 
columns = [
    'age', 'job', 'marital', 'education', 'default', 'balance', 'housing',
    'loan', 'contact', 'day', 'month', 'duration', 'campaign', 'pdays',
    'previous', 'poutcome', 'y'
]

try:
    # 4. Create Spark dataframe 
    df_baru = spark.createDataFrame(newData, schema=columns)
    
    print("Data baru yang akan ditambahkan:")
    display(df_baru)

    # 5. append data 
    print(f"\nMenambahkan data ke tabel: {TABLE_TO_APPEND}...")
    
    df_baru.write \
        .mode("append") \
        .saveAsTable(TABLE_TO_APPEND)
        
    print("Data baru telah berhasil ditambahkan.")

    # cek data terbaru 
    print(f"\nVerifikasi: 5 baris terakhir dari {TABLE_TO_APPEND}")
    
    df_total = spark.read.table(TABLE_TO_APPEND)
    display(df_total.tail(5)) # Anda akan melihat data umur xx and yy di sini

except Exception as e:
    print(f"Error: {e}")
    print("Pastikan nama tabel dan nama kolom sudah benar.")