stds.reql = {
  read_globals = {
    string = {
       read_only = true,
       fields = {
          pack = {read_only = true},
          unpack = {read_only = true}
       }
    },
    table = {
       read_only = true,
       fields = {
          unpack = {read_only = true}
       }
    }
  }
}
std = 'min'
exclude_files = {'here'}
files['spec'] = {
  max_line_length = false,
  std = '+busted'
}
files['src'] = {std = '+reql'}
