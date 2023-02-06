from dataflows import Flow, load, dump_to_path, add_metadata, printer, update_resource, unpivot
from dataflows.helpers import ResourceMatcher
import csv 
import openpyxl
import os


def rename_column(from_name, to_name, resources=None):
    def renamer(rows):
        for row in rows:
            yield dict(
                (k if k != from_name else to_name, v) for k, v in row.items()
            )

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                for field in resource.get("schema", {}).get("fields", []):
                    if field["name"] == from_name:
                        field["name"] = to_name
        yield package.pkg
        for res in package:
            if matcher.match(res.res.name):
                yield renamer(res)
            else:
                yield res

    return func

def xlsx_to_csv():
    inputExcelFile = 'archive/work_stats.xlsx'
    newWorkbook = openpyxl.load_workbook(inputExcelFile)
    firstWorksheet = newWorkbook.active
    OutputCsvFile = csv.writer(open("data/work_stats.csv", 'w'), delimiter=",")
    for eachrow in firstWorksheet.rows:
        OutputCsvFile.writerow([cell.value for cell in eachrow])

def remove_last_column():
    with open("data/work_stats.csv","r") as fin, open("data/work_stats_out.csv","w") as fout:
        writer=csv.writer(fout)
        #next(csv.reader(fin), None)  # skip the headers
        for row in csv.reader(fin):
            writer.writerow(row[:-1])

def work_indications_process():
    xlsx_to_csv()
    remove_last_column()
    unpivoting_fields = [
        { 'name': '([0-9]{4})', 'keys': {'year': r'\1'} }
    ]
    extra_keys = [ {'name': 'year', 'type': 'year'} ]
    extra_value = {'name': 'value', 'type': 'string'}
    with open("data/work_stats_out.csv", 'r') as file_in, open("data/work_stats_out_final.csv", 'w') as file_out:
        csvreader = csv.reader(file_in)
        csvwriter = csv.writer(file_out)
        sc = "sector"
        for row in csvreader:
            if len(row[2]) == 0:
                sc = row[0]
            if len(row[2]) > 0:
                row.append(sc)
                csvwriter.writerow(row)
    
    flow = Flow(
        load("data/work_stats_out_final.csv", format='csv', name='decent-work-indicators'),
        unpivot(unpivoting_fields, extra_keys, extra_value),
        rename_column("Перечень показателей, рекомендуемых Международной организацией труда*", "segment", "decent-work-indicators"),
        rename_column("sector", "indicators", "decent-work-indicators"),
        update_resource('decent-work-indicators', path='data/decent-work-indicators.csv'),
        add_metadata(name='decent-work-indicators', title='''Decent work indicators in Kazakhstan by segment.'''),
        printer(),
        dump_to_path(),
    )
    flow.process()
    os.remove("data/work_stats.csv")
    os.remove("data/work_stats_out.csv")
    os.remove("data/work_stats_out_final.csv")
if __name__ == '__main__':
    work_indications_process()