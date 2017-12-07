import pandas as pd
import glob
import osr
import ogr
import csv

#author Luke Burton
#file format is yyyymm_Month
file_path_import = '/home/mint/luke/shape_file_import/topports/ais_fileration_by_month/201612_December'
export_to_csv = '/home/mint/luke/shape_file_import/Shapefiles/CSV_output/201612_December.csv'
file_path_export = '/home/mint/luke/shape_file_import/Shapefiles/201612_December/201612_December_top_ports.shp'


target_dict={}


csvFiles = glob.glob(file_path_import + "/*/*.csv") #finds all csv's in folder

for file_ in csvFiles:

    with open(file_, "r") as csvfile:
        vtype_name = file_.split("/")[-2].split("_")[1].split(".tsv")[0] #creates the vessel type from filepath

        ff = csv.reader(csvfile)
        for d in ff:
            cell = d[0]

            if cell in target_dict:

                target_dict[cell][vtype_name] = d[1]

            else:
                target_dict[d[0]] = {vtype_name: d[1]} #adds each vessel type as a column header
#print(target_dict)


df_count = pd.DataFrame.from_dict(target_dict, orient='index')
df_count = df_count.reindex_axis(sorted(df_count.columns), axis=1) #sort columns
df_count = df_count['BulkCarriers':].astype(str).astype(float) #convet from object to float
df_count['Total'] = df_count.sum(axis=1) - df_count['gt2000'] - df_count['gt300'] #add total column (minus gt2000 and gt300)
df_count = df_count.sort_values('Total', ascending=False)
df_count = df_count.reset_index() #add index
df_count = df_count.rename(columns={ df_count.columns[0]: "cell_id" }) #rename cell_id



#merge with avcs naming csv
file_path = '/home/mint/luke/shape_file_import/avcs_cat_full.csv'
df_cell_name = pd.read_csv(file_path, names=['cell_id', 'cell_name'])
df_cell_name = pd.merge(df_count, df_cell_name, on='cell_id', how='inner')


#merge with wkts csv
file_path = '/home/mint/luke/avcs_cat.csv'
df_wkt = pd.read_csv(file_path, names=['cell_id', 'wkt'])
df = pd.merge(df_cell_name, df_wkt, on='cell_id', how='inner')


df.to_csv(export_to_csv, encoding='utf-8') #convert to csv to create shapefile from


#convert to shapefile
spatialref = osr.SpatialReference()  # Set the spatial ref
spatialref.SetWellKnownGeogCS('WGS84')

driver = ogr.GetDriverByName("ESRI Shapefile")
dstfile = driver.CreateDataSource(file_path_export) # Output file

dstlayer = dstfile.CreateLayer("layer", spatialref, geom_type=ogr.wkbPolygon)

# Add the other attribute fields
def create_shapefile_attributes():
    fielddef = ogr.FieldDefn("Rank", ogr.OFTInteger)
    fielddef.SetWidth(10)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("Cell_id", ogr.OFTString)
    fielddef.SetWidth(80)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("BulkCarriers", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("DryCargoPassenger", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("Fishing", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("Miscellaneous", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("NonMerchantShips", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("NonPropelled", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("NonSeagoingMerchantShips", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("NonShipStructures", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("Offshore", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("Tankers", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("gt2000", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("gt300", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("Total", ogr.OFTInteger)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

    fielddef = ogr.FieldDefn("Cell_title", ogr.OFTString)
    fielddef.SetWidth(100)
    dstlayer.CreateField(fielddef)

# Read csv file
def csv_2_shp():
    with open(export_to_csv) as file_input:
        reader = csv.reader(file_input)
        next(reader)
        for nb, row in enumerate(reader):
            # WKT is in the second field in my test file :
            poly = ogr.CreateGeometryFromWkt(row[16])
            feature = ogr.Feature(dstlayer.GetLayerDefn())
            feature.SetGeometry(poly)
            feature.SetField("BulkCarrie", row[2])
            feature.SetField("DryCargoPa", row[3])
            feature.SetField("Fishing", row[4])
            feature.SetField("Miscellane", row[5])
            feature.SetField("NonMerchan", row[6])
            feature.SetField("NonPropell", row[7])
            feature.SetField("NonSeagoin", row[8])
            feature.SetField("NonShipStr", row[9])
            feature.SetField("Offshore", row[10])
            feature.SetField("Tankers", row[11])
            feature.SetField("gt2000", row[12])
            feature.SetField("gt300", row[13])
            feature.SetField("Total", row[14])
            feature.SetField("Rank", nb)
            feature.SetField("Cell_id", row[1])
            feature.SetField("Cell_title", row[15])
            dstlayer.CreateFeature(feature)

create_shapefile_attributes()

csv_2_shp()





