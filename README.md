# Whitebread Fedora

Simple python scripts for those everyday, mundane Fedora tasks.

## Installing

## Harvesting Metadata!

**By Matching Parent Namespace:**
```
>>> python harvest_metadata.py -p smhc 
```

**By Dublincore Field Matching:**

```
>>> python harvest_metadata.py -dc rights -dcs "In Copyright"
```

**Override Default DSID without Touching YAML:**

```
>>> python harvest_metadata.py -p smhc -ds DC
```

## Downloading Binaries!

## Updating GSearch!