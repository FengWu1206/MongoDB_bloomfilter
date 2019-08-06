from bloom_filter import BloomFilter

tree = BloomFilter(max_elements=2**16, error_rate=0.1)
tree.add("nihao")
tree.add("sleep")
print(tree)
btree = {}
btree["a.fn"] = tree
print(btree)