# Enable pending breakpoints
set breakpoint pending on

# Initialize counters
set $HG_Create_count = 0
set $HG_Destroy_count = 0
set $HG_Context_create_count = 0
set $HG_Context_destroy_count = 0
set $HG_Bulk_create_count = 0
set $HG_Bulk_free_count = 0

# Breakpoint for HG_Create
break HG_Create
commands
  silent
  set $HG_Create_count = $HG_Create_count + 1
  continue
end

# Breakpoint for HG_Destroy
break HG_Destroy
commands
  silent
  set $HG_Destroy_count = $HG_Destroy_count + 1
  continue
end

# Breakpoint for HG_Context_create
break HG_Context_create
commands
  silent
  set $HG_Context_create_count = $HG_Context_create_count + 1
  continue
end

# Breakpoint for HG_Context_destroy
break HG_Context_destroy
commands
  silent
  set $HG_Context_destroy_count = $HG_Context_destroy_count + 1
  continue
end

# Breakpoint for HG_Bulk_create
break HG_Bulk_create
commands
  silent
  set $HG_Bulk_create_count = $HG_Bulk_create_count + 1
  continue
end

# Breakpoint for HG_Bulk_free
break HG_Bulk_free
commands
  silent
  set $HG_Bulk_free_count = $HG_Bulk_free_count + 1
  continue
end

# Start the program
run

# After the run, print the counters
printf "HG_Create was called %d times\n", $HG_Create_count
printf "HG_Destroy was called %d times\n", $HG_Destroy_count
printf "HG_Context_create was called %d times\n", $HG_Context_create_count
printf "HG_Context_destroy was called %d times\n", $HG_Context_destroy_count
printf "HG_Bulk_create was called %d times\n", $HG_Bulk_create_count
printf "HG_Bulk_free was called %d times\n", $HG_Bulk_free_count