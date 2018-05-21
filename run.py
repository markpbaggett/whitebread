import yaml
import argparse
from fedora import Set

def choose_operation(choice, instance, ds=None, predicate=None, xpath=None):
    if choice == "grab_images":
        instance.grab_images(ds)
    elif choice == "update_gsearch":
        instance.update_gsearch()
    elif choice == "harvest_metadata":
        instance.harvest_metadata(ds)
        print(f"\n\nDownloaded {len(instance.results)} {ds} records.")
    elif choice == "find_missing":
        instance.mark_as_missing(ds)
    elif choice == "list_dsids":
        instance.list_dsids()
    elif choice == "get_relationships":
        instance.get_relationships()
    elif choice == "grab_other":
        instance.grab_other(ds)
    elif choice == "find_matching_relationship":
        memberships = instance.find_rels_ext_relationship(predicate)
        print(memberships)
    elif choice == "update_labels":
        if xpath is not None:
            instance.update_fgs_label(xpath)
        else:
            print("Must specify xpath value.")
    elif choice == "harvest_metadata_no_pages":
        memberships = instance.find_rels_ext_relationship("isMemberOf")
        for pid in memberships:
            instance.results.remove(pid["pid"])
        instance.harvest_metadata(ds)
        print(f"\n\nDownloaded {len(instance.results)} {ds} records.")
    elif choice == "find_bad_books":
        if predicate is None:
            predicate = "isMemberOf"
        all_memberships = instance.find_rels_ext_relationship(predicate)
        objects_missing_dsid = instance.mark_as_missing(ds)
        items_to_remove = []
        for i in objects_missing_dsid:
            x = review_memberships(i, all_memberships, predicate)
            if x not in items_to_remove and x is not None:
                items_to_remove.append(x)
        for i in all_memberships:
            for j in items_to_remove:
                if i[predicate] == j and i["pid"] not in items_to_remove:
                    items_to_remove.append(i["pid"])
        print(f"Here is a list of objects that have parts missing a {ds}:")
        total = 1
        for i in items_to_remove:
            print(f"{total}. {i}")
            total += 1
    else:
        print("No valid operator.")

def review_memberships(item, membership_list, rel):
    for i in membership_list:
        if i["pid"] == item:
            return i[rel]


def main():
    parser = argparse.ArgumentParser(description='Use to specify a collection')
    parser.add_argument("-p", "--parentnamespace", dest="parent_namespace", help="parent namespace of collection")
    parser.add_argument("-dc", "--dcfield", dest="dc_field", help="grab pids according to dc field")
    parser.add_argument("-dcs", "--dcstring", dest="dc_string", help="specify a dc string")
    parser.add_argument("-ds", "--dsid", dest="datastream_id", help="specify text datastream.")
    parser.add_argument("-o", "--operation", dest="operation", help="Choose one: grab_images, harvest_metadata, "
                                                                    "grab_other, update_gsearch, find_missing, "
                                                                    "get_relationships, find_bad_books, update_labels,"
                                                                    "harvest_metadata_no_pages",required=True)
    parser.add_argument("-r", "--relationship", dest="relationship", help="Specify the relationship to check for.")
    parser.add_argument("-xp", "--xpath", dest="xpath", help="Specify an xpath value to find. Used in update_label.")
    args = parser.parse_args()

    settings = yaml.load(open("config.yml", "r"))

    fedora_collection = dc_parameter = ""
    relationship = None
    if args.relationship:
        relationship = args.relationship
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
    my_xpath = None
    if args.xpath:
        my_xpath = args.xpath
    my_request = f"{fedora_url}:8080/fedora/objects?query={fedora_collection}{dc_parameter}" \
                 f"&pid=true&resultFormat=xml".replace(" ", "%20")
    my_records = Set(my_request, settings)
    my_records.populate()
    choose_operation(operation, my_records, dsid, relationship, my_xpath)

if __name__ == "__main__":
    main()
