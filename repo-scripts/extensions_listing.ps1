code --list-extensions | ForEach-Object { "https://marketplace.visualstudio.com/items?itemName=$_" } > extensions_list.txt
code --list-extensions > extensions_list_names.txt