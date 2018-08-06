import requests
import yaml

settings = yaml.load(open("../config.yml", "r"))


def purge_object(pid):
    log_message = f"Purging {pid}."
    r = requests.delete(f"{settings['fedora_path']}:8080/fedora/objects/{pid}?{log_message}", auth=(settings['username'], settings['password']))
    if r.status_code == 200:
        print(log_message)
    else:
        print(f"Could not purge {pid}. Status code: {r.status_code}.")

with open("../delete.txt", "r") as list_of_pids:
    for current_pid in list_of_pids:
        purge_object(current_pid.replace("\n", ""))