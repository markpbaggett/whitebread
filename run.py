import yaml
import argparse
from app.fedora import Set, Record


def choose_operation(choice, instance, ds=None, predicate=None, xpath=None):
    if choice == "grab_images":
        instance.grab_images(ds)
    elif choice == "update_gsearch":
        instance.update_gsearch()
    elif choice == "update_gsearch_no_pages":
        memberships = instance.find_rels_ext_relationship("isMemberOf")
        for pid in memberships:
            instance.results.remove(pid["pid"])
        instance.update_gsearch()
    elif choice == "grab_foxml":
        instance.grab_foxml()
    elif choice == "harvest_metadata":
        instance.harvest_metadata(ds)
    elif choice == "find_missing":
        instance.mark_as_missing(ds)
    elif choice == "list_dsids":
        instance.list_dsids()
    elif choice == "get_relationships":
        instance.get_relationships()
    elif choice == "grab_other":
        instance.grab_other(ds)
    elif choice == "find_content_type":
        instance.find_content_types()
    elif choice == "write_results":
        instance.write_results_to_file()
    elif choice == "test_obj_mimes":
        x = instance.check_obj_mime_types()
        print("\nHere are the unique mime types in your result set:")
        for k, v in x.items():
            print(f"\tThere are {v} OBJs that are {k}.")
    elif choice == "find_matching_relationship":
        memberships = instance.find_rels_ext_relationship(predicate)
        print(memberships)
    elif choice == "update_labels":
        if xpath is not None:
            for result in instance.results:
                new_record = Record(result)
                relationships = new_record.find_rels_ext_relationship("isMemberOf")
                if relationships is not None:
                    print(f"Finding parent of page {result}.")
                    parent = Record(relationships["isMemberOf"])
                    label = parent.get_parent_label(xpath)
                    new_record.update_fgs_label(xpath, f"{label}:  page {relationships['page number']}")
                else:
                    new_record.update_fgs_label(xpath)
        else:
            print("Must specify xpath value.")
    elif choice == "harvest_metadata_no_pages":
        memberships = instance.find_rels_ext_relationship("isMemberOf")
        for pid in memberships:
            instance.results.remove(pid["pid"])
        instance.harvest_metadata(ds)
    elif choice == "find_bad_books":
        # Set some variables
        if predicate is None:
            predicate = "isMemberOf"
        items_to_remove = []
        book_objects_to_remove = []
        # Find all memberships for results matching query
        all_memberships = instance.find_rels_ext_relationship(predicate)
        # Find objects missing the datastream in question
        objects_missing_dsid = instance.mark_as_missing(ds)
        # Add objects missing the dsid in question if they aren't already queued for removal
        for i in objects_missing_dsid:
            x = review_memberships(i, all_memberships, predicate)
            if x not in items_to_remove and x is not None:
                items_to_remove.append(x)
                book_objects_to_remove.append(x)
        for i in all_memberships:
            for j in items_to_remove:
                if i[predicate] == j and i["pid"] not in items_to_remove:
                    items_to_remove.append(i["pid"])
        print(f"Here is a list of objects that have parts missing a {ds}:")
        total = 1
        with open(f"pids_to_delete.txt", "w") as my_bad_pids:
            for i in items_to_remove:
                print(f"{total}. {i}")
                my_bad_pids.write(f"{i}\n")
                total += 1
        print(f"\nThese are the book objects that have some bad pages:")
        book_total = 1
        for i in book_objects_to_remove:
            print(f"{book_total}. {i}")
            book_total += 1
    elif choice == "count_objects":
        print(f"\n\nTotal matching documents: {instance.count_objects()}")
    elif choice == "test_embargos":
        instance.test_embargos()
    elif choice == "purge_old_dsids":
        if ds is not None:
            instance.purge_all_but_newest_dsid(ds)
        else:
            print("\n\nYou need to define a datastream to purge.")
    else:
        print("No valid operator.")
    return


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
                                                                    "get_relationships, find_bad_books, update_labels, "
                                                                    "harvest_metadata_no_pages, grab_foxml, "
                                                                    "count_objects, update_gsearch_no_pages, "
                                                                    "purge_old_dsids, write_results",
                        required=True)
    parser.add_argument("-r", "--relationship", dest="relationship", help="Specify the relationship to check for.")
    parser.add_argument("-xp", "--xpath", dest="xpath", help="Specify an xpath value to find. Used in update_label.")
    args = parser.parse_args()

    settings = yaml.load(open("config.yml", "r"))

    fedora_collection = dc_parameter = ""
    relationship = dsid = my_xpath = None
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
    if args.datastream_id:
        dsid = args.datastream_id
    if args.xpath:
        my_xpath = args.xpath
    my_request = f"{fedora_url}:8080/fedora/objects?query={fedora_collection}{dc_parameter}" \
                 f"&pid=true&resultFormat=xml&maxResults={settings['max_results']}".replace(" ", "%20")
    my_records = Set(my_request, settings)
    print("\nPopulating results set.", end="", flush=True)
    while my_records.token is not None:
        my_records.populate()
    choose_operation(operation, my_records, dsid, relationship, my_xpath)


if __name__ == "__main__":
    main()
