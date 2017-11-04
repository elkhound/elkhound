import datetime
import elkhound
import os
import pandas as pd


class DownloadDataTask(elkhound.Task):
    def get_input_data_file_codes(self):
        return []

    def get_output_data_file_codes(self):
        return [1230, 2110]

    def run(self, input_files, output_files, context=None):
        # Generate D1230
        df_1230 = pd.DataFrame(data = {
            'name': ['Jane Doe', 'Mark Smith'],
            'dob': [datetime.datetime(year=1980, month=2, day=3), datetime.datetime(year=1970, month=4, day=5)],
            'is_employee': [True, False],
        }, columns=['name', 'dob', 'is_employee'])
        output_files[1230].write_data_frame(df_1230)

        # Generate D2110
        df_2110 = pd.DataFrame(data={
            'foo': [1, 2, 5],
        })
        writer = pd.ExcelWriter(output_files[2110].get_path())
        df_2110.to_excel(writer)
        writer.save()


class GenerateReportTask(elkhound.Task):
    def get_input_data_file_codes(self):
        return [1230, 2110]

    def get_output_data_file_codes(self):
        return [4315]

    def run(self, input_files, output_files, context=None):
        is_extended = int(context['generate_report.extended_report'])
        with output_files[4315].open() as f:
            df = input_files[1230].read_data_frame()
            f.write('{} people in database, including {} employees\n'.format(
                len(df.index),
                len(df[df.is_employee == 1].index),
            ))
            if is_extended:
                for _, input_file in input_files.items():
                    f.write('Used input file {}\n'.format(input_file.get_path()))


class PlotBudgetTask(elkhound.Task):
    def get_input_data_file_codes(self):
        return [2110]

    def get_output_data_file_codes(self):
        return [5214]

    def run(self, input_files, output_files, context=None):
        os.makedirs(output_files[5214].get_path())
