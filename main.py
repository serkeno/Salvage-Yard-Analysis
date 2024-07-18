import JSONFileBuilder as Jfb
import StoredQueries as SQ

if __name__ == "__main__":
    Jfb.build_file(dataframe=SQ.vehicle_presence(), name="binary_presence_type_model")
    # Jfb.build_continuous_vehicle_presence()
    # Jfb.build_binary_vehicle_presence()



