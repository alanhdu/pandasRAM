Short hack-ish script for storing Pandas PyTable format. 
Want to store large dataframes and query it, without
using lots of RAM. Unfortunately, dataframes contain 
variable-length strings (up to 10000 chars long), so we
can't use the built-in table format.
