import sys
import zlib


text=b"""Thisdsadasdasd function is the primary interface to this module along with 
decompress() function. This function returns byte object by compressing the data 
given to it as parameter. The function has another parameter called level which 
controls the extent of compression. It an integer between 0 to 9. Lowest value 0 
stands for no compression and 9 stands for best compression. Higher the level of 
compression, greater the length of compressed byte object."""

# Checking size of text
text_size=sys.getsizeof(text)
print("\nsize of original text",text_size)

# Compressing text
compressed = zlib.compress(text)

# Checking size of text after compression
csize=sys.getsizeof(compressed)
print("\nsize of compressed text",csize)

# Decompressing text
decompressed=zlib.decompress(compressed)

#Checking size of text after decompression
dsize=sys.getsizeof(decompressed)
print("\nsize of decompressed text",dsize)

print("\nDifference of size= ", text_size-csize)