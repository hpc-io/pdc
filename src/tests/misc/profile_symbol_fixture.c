/* A foreign library with names also used by the PDC profiler. */
static int init_calls, fini_calls;
void
profile_init(void)
{
    init_calls++;
}
void
finalize_profile(void)
{
    fini_calls++;
}
int
foreign_profile_init_calls(void)
{
    return init_calls;
}
int
foreign_profile_fini_calls(void)
{
    return fini_calls;
}
