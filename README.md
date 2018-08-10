# Whitebread Fedora

Simple python scripts for those everyday, mundane Fedora tasks.

## Count Matching Objects

```
>>> python run.py -o count_objects -dc type -dcs StillImage
```

```
>>> python run.py -o count_objects -p vanvactor
```

## Harvest Metadata!

**By Matching Parent Namespace:**
```
>>> python run.py -p smhc -o harvest_metadata
```

**By Dublincore Field Matching:**

```
>>> python run.py -o harvest_metadata -dc rights -dcs "In Copyright"
```

**Override Default DSID without Touching YAML:**

```
>>> python run.py -o harvest_metadata -p smhc -ds DC
```

## Harvest Metadata but Ignore Records for Pages

```
>>> python run.py -o harvest_metadata_no_pages -p smhc -ds MODS
```

## Grab Images

**Like above, but for image mime types**
```
>>> python run.py -p smhc -o grab_images -ds JP2
```

## Download Binaries!

**Just like above examples but with a different operator (use for things that aren't images or test).**

```
>>> python run.py -o grab_other -p smhc -ds PDF
```

## Update GSearch!

**You guessed it!**

```
>>> python run.py -o update_gsearch -p smhc
```

## Update GSearch for all objects that aren't pages

```
>>> python run.py -o update_gsearch_no_pages -p smhc
```

## Update fgsLabel!

**Like above, but drop in an xpath value to match on. Only works with mods right now."**

```
>>> python run.py -o update_labels -p swim -xp "//mods:titleInfo[@supplied='yes']/mods:title"
```

## Purge All But the Newest Version of a Datastream

```
>>> python run.py -o purge_old_dsids -p vanvactor -ds MODS
```

## Check MimeType of the Preservation Object

```
>>> python run.py -o test_obj_mimes -p vanvactor
```

## Find matching objects missing a specific datastream

```
>>> python run.py -o find_missing -p vanvactor -ds PDF
```

## Download FOXML for matching documents

```
>>> python run.py -o grab_foxml -p vanvactor
```

## Find Books that are bad or aren't done processing

```
>>> python run.py -o find_bad_books -p vanvactor
```