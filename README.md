# Netflix Movies and TV Shows ETL

Proyek ini membangun **ETL Pipeline** untuk dataset [Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows).  
Tujuan utama: membersihkan data mentah CSV, melakukan transformasi (tanggal, tahun rilis, genre, negara, sutradara), lalu memuat hasilnya ke database **SQLite** yang siap dianalisis.

## Fitur
- Extract data dari Kaggle atau file CSV.
- Transformasi: parsing tanggal, normalisasi tahun, hapus duplikat, pecah genre & negara.
- Load ke database SQLite (mudah dijalankan di lokal).
- Contoh query SQL untuk analisis dasar.

## Cara Jalankan
1. Buat virtual environment dan install dependensi (`requirements.txt`).
2. Download dataset Netflix dari Kaggle dan simpan di folder `data/`.
3. Jalankan pipeline ETL untuk membuat database `netflix.db`.

## Hasil
- Database SQLite dengan tabel terstruktur.
- Data Netflix siap dianalisis dengan SQL atau divisualisasikan dengan Python.

---

Dataset © [Kaggle - Shivam Bansal](https://www.kaggle.com/datasets/shivamb/netflix-shows)  
Proyek ini dibuat untuk tujuan edukasi.
