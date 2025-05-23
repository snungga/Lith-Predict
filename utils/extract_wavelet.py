import os
import pandas as pd
from segysak.segy import segy_loader, well_known_byte_locs


class SeismicProcessor:
    def __init__(self, directory_path, variable_names, well_inline, well_crossline, tag="F02", new_col_names=None):
        """
        Inisialisasi processor untuk file SEGY.
        
        Args:
            directory_path (str): Path ke folder berisi file SEGY.
            variable_names (list of str): Nama variabel SEGY yang ingin diproses.
            well_inline (int): Lokasi inline well.
            well_crossline (int): Lokasi crossline well.
            tag (str): Suffix untuk hasil slicing (default "F02").
            new_col_names (dict): Opsional, dict mapping nama variabel hasil ke nama kolom baru.
        """
        self.directory_path = directory_path
        self.variable_names = variable_names
        self.well_inline = well_inline
        self.well_crossline = well_crossline
        self.tag = tag
        self.new_col_names = new_col_names or {}
        self.results = {}

    def load_segy_files(self):
        """
        Membaca semua file .segy/.sgy dari direktori dan menyimpannya ke variabel global.
        """
        for filename in os.listdir(self.directory_path):
            if filename.endswith(".segy") or filename.endswith(".sgy"):
                var_name = os.path.splitext(filename)[0]
                file_path = os.path.join(self.directory_path, filename)

                data = segy_loader(
                    file_path,
                    **well_known_byte_locs("petrel_3d")
                )

                globals()[var_name] = data
                self.variable_names.append(var_name)
                print(f"'{var_name}' telah dimuat dari {filename}.")

    def process(self):
        """
        Slice semua variabel berdasarkan posisi well dan simpan hasil ke globals dan self.results.
        Rename kolom 'data' jika ada mapping di self.new_col_names.
        """
        for var_name in self.variable_names:
            if var_name in globals():
                data = globals()[var_name]
                print(f"Proses data pada {var_name}...")

                # slicing berdasarkan well location
                loc_well = data.sel(
                    iline=self.well_inline,
                    xline=self.well_crossline,
                    method="nearest"
                ).to_dataframe().reset_index()

                new_var_name = f"{var_name}_{self.tag}"

                # rename kolom jika diperlukan
                if new_var_name in self.new_col_names and 'data' in loc_well.columns:
                    rename_to = self.new_col_names[new_var_name]
                    loc_well = loc_well.rename(columns={'data': rename_to})
                    print(f"Kolom 'data' pada {new_var_name} diubah menjadi '{rename_to}'.")

                self.results[new_var_name] = loc_well
                globals()[new_var_name] = loc_well

                print(f"{new_var_name} telah dibuat dan disimpan.")
            else:
                print(f"Variabel {var_name} tidak ditemukan di globals.")

    def get_result(self, var_name):
        """
        Mengambil hasil slice dari variabel tertentu.
        """
        return self.results.get(f"{var_name}_{self.tag}", None)

    def export_all_to_csv(self, output_dir):
        """
        Mengekspor semua hasil slice ke file CSV di folder tujuan.
        """
        os.makedirs(output_dir, exist_ok=True)
        for name, df in self.results.items():
            csv_path = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Hasil {name} disimpan ke {csv_path}")

    def list_loaded(self):
        """
        Menampilkan semua nama variabel yang berhasil diproses.
        """
        return list(self.results.keys())
