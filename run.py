import yaml
import argparse
from fedora import Set

def choose_operation(choice, instance, ds=None):
    if choice == "grab_binaries":
        instance.grab_binaries(ds)
    elif choice == "update_gsearch":
        instance.update_gsearch()
    elif choice == "harvest_metadata":
        instance.harvest_metadata(ds)
    else:
        print("No valid operator.")



def main():
    parser = argparse.ArgumentParser(description='Use to specify a collection')
    parser.add_argument("-p", "--parentnamespace", dest="parent_namespace", help="parent namespace of collection")
    parser.add_argument("-dc", "--dcfield", dest="dc_field", help="grab pids according to dc field")
    parser.add_argument("-dcs", "--dcstring", dest="dc_string", help="specify a dc string")
    parser.add_argument("-ds", "--dsid", dest="datastream_id", help="specify text datastream.")
    parser.add_argument("-o", "--operation", dest="operation", help="Choose one: grab_binaries, harvest_metadata, "
                                                                    "update_gsearch", required=True)
    args = parser.parse_args()

    settings = yaml.load(open("config.yml", "r"))

    fedora_collection = dc_parameter = ""
    fedora_url = settings["fedora_path"]
    if not fedora_url.startswith("http"):
        fedora_url = f"http://{fedora_url}"
    if args.parent_namespace:
        fedora_collection = f"pid%7E{args.parent_namespace}*"
    operation = args.operation
    if args.dc_field and args.dc_string:
        dc_parameter = f"{args.dc_field}%7E%27{args.dc_string}%27"
    elif args.dc_field or args.dc_string:
        print(f"Must include both a dc field and a dc string.")
    dsid = None
    if args.datastream_id:
        dsid = args.datastream_id

    my_request = f"{fedora_url}:8080/fedora/objects?query={fedora_collection}{dc_parameter}" \
                 f"&pid=true&resultFormat=xml".replace(" ", "%20")
    my_records = Set(my_request, settings)
    my_records.populate()
    choose_operation(operation, my_records, dsid)

if __name__ == "__main__": main()
