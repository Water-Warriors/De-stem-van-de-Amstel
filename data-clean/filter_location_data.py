import pandas as pd
import geopandas as gpd


def filter_points_by_distance(points_gdf, area_gdf, distance_meters=50):
    """
    Selects points that are within a specified distance of an area's geometry.

    This function performs these steps:
    1.  Uses a projected Coordinate Reference System (CRS) suitable for the Netherlands (EPSG:28992)
        to ensure distance calculations are accurate in meters.
    2.  Creates a "buffer" polygon around the area_gdf geometry.
    3.  Selects all points from points_gdf that fall within this buffer zone.
    4.  Returns the filtered points in their original CRS.

    Args:
        points_gdf (GeoDataFrame): The GeoDataFrame of points to be filtered.
        area_gdf (GeoDataFrame): The GeoDataFrame of the area (e.g., a river) to buffer.
        distance_meters (int): The distance in meters for the buffer.

    Returns:
        GeoDataFrame: A new GeoDataFrame containing only the points within the buffer.
    """
    print(f"\nFiltering points within {distance_meters} meters of the target area...")

    # Use a projected CRS for accurate distance calculations in meters (RD New for Netherlands)
    metric_crs = "EPSG:28992"

    # Re-project both GeoDataFrames to the metric CRS
    try:
        points_metric = points_gdf.to_crs(metric_crs)
        area_metric = area_gdf.to_crs(metric_crs)
    except Exception as e:
        print(f"❌ CRS Error: Could not re-project data. Ensure input data has a valid CRS set. Original error: {e}")
        return gpd.GeoDataFrame()  # Return empty frame on error

    # Create a single, unified buffer polygon around the area's geometry
    # .unary_union merges all shapes in the GeoDataFrame into one for a clean buffer
    area_buffer = area_metric.geometry.buffer(distance_meters).union_all()

    # Find the points that are spatially 'within' the buffer
    points_within_buffer_metric = points_metric[points_metric.within(area_buffer)]

    # Get the original indices from the points that were found
    original_indices = points_within_buffer_metric.index

    print(f"✅ Found {len(original_indices)} points within the specified distance.")

    # Return the filtered points from the original GeoDataFrame to preserve original columns and CRS
    return points_gdf.loc[original_indices]


# --- Main script execution ---
if __name__ == "__main__":

    # --- ⚙️ 1. CONFIGURATION ---
    # Update these file paths and settings to match your data.

    POINTS_FILEPATH = 'cleaned_pfas_data_tidy.csv'  # Your CSV file with point data
    SHAPEFILE_FILEPATH = 'amstelland_shapefile/OWMIDENT_NL11_1_1_clean.shp'  # Your shapefile defining the area
    OUTPUT_FILEPATH = 'points_near_amstel.csv'  # Name of the file for the results

    LON_COLUMN = 'lon'  # Name of the longitude column in your CSV
    LAT_COLUMN = 'lat'  # Name of the latitude column in your CSV

    DISTANCE_IN_METERS = 1000  # The distance to search around the shapefile area

    # --- 🚀 2. RUNNING THE ANALYSIS ---
    try:
        # Load the shapefile of the area (e.g., the Amstel river system)
        print(f"Loading shapefile from '{SHAPEFILE_FILEPATH}'...")
        amstel_area_gdf = gpd.read_file(SHAPEFILE_FILEPATH)

        # Load your dataset with lon/lat points
        print(f"Loading points from '{POINTS_FILEPATH}'...")
        points_df = pd.read_csv(POINTS_FILEPATH)

        # Prepare the points GeoDataFrame
        print("Converting points to a GeoDataFrame...")
        points_gdf = gpd.GeoDataFrame(
            points_df,
            geometry=gpd.points_from_xy(points_df[LON_COLUMN], points_df[LAT_COLUMN])
        )
        # Set the initial CRS for your lat/lon data (WGS84 is standard for GPS)
        points_gdf.set_crs("EPSG:4326", inplace=True)

        # Call the function to get only the points near the Amstel
        points_near_amstel = filter_points_by_distance(
            points_gdf,
            amstel_area_gdf,
            distance_meters=DISTANCE_IN_METERS
        )

        # --- 💾 3. VIEWING AND SAVING RESULTS ---
        print("\n--- Filtered Results ---")
        print(f"Original number of points: {len(points_gdf)}")
        print(f"Number of points near Amstel: {len(points_near_amstel)}")

        if not points_near_amstel.empty:
            print("\nPreview of filtered data:")
            print(points_near_amstel.head())

            # Save your filtered data to a new file
            points_near_amstel.to_csv(OUTPUT_FILEPATH, index=False)
            print(f"\n🎉 Success! Filtered data has been saved to '{OUTPUT_FILEPATH}'.")
        else:
            print("\nNo points were found within the specified distance. No output file was created.")

    except FileNotFoundError as e:
        print(f"\n❌ Error: File not found. Please check your file paths in the CONFIGURATION section.")
        print(f"Missing file: {e.filename}")
    except KeyError as e:
        print(f"\n❌ Error: A column name was not found in your CSV.")
        print(f"Please make sure the LON_COLUMN ('{LON_COLUMN}') and LAT_COLUMN ('{LAT_COLUMN}') are correct.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")