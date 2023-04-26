#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mpi.h>

using namespace std;

int main(int argc, char *argv[]);
void timestamp();

int main(int argc, char *argv[])

{
    int id;
    int ierr;
    int p;
    double wtime;

    ierr = MPI_Init(&argc, &argv);

    if (ierr != 0)
    {
        cout << "\n";
        cout << "HELLO_MPI - Fatal error!\n";
        cout << "  MPI_Init returned nonzero ierr.\n";
        exit(1);
    }

    ierr = MPI_Comm_size(MPI_COMM_WORLD, &p);

    ierr = MPI_Comm_rank(MPI_COMM_WORLD, &id);

    if (id == 0)
    {
        timestamp();
        cout << "\n";
        cout << "P" << id << ":  HELLO_MPI - Master process:\n";
        cout << "P" << id << ":    C++/MPI version\n";
        cout << "P" << id << ":    An MPI example program.\n";
        cout << "\n";
        cout << "P" << id << ":    The number of processes is " << p << "\n";
        cout << "\n";
    }
    //
    //  Every process prints a hello.
    //
    if (id == 0)
    {
        wtime = MPI_Wtime();
    }
    cout << "P" << id << ":    'Hello, world!'\n";

    if (id == 0)
    {
        wtime = MPI_Wtime() - wtime;
        cout << "P" << id << ":    Elapsed wall clock time = " << wtime << " seconds.\n";
    }

    MPI_Finalize();

    if (id == 0)
    {
        cout << "\n";
        cout << "P" << id << ":  HELLO_MPI:\n";
        cout << "P" << id << ":    Normal end of execution.\n";
        cout << "\n";
        timestamp();
    }
    return 0;
}

void timestamp()

{
#define TIME_SIZE 40

    static char time_buffer[TIME_SIZE];
    const struct std::tm *tm_ptr;
    std::time_t now;

    now = std::time(NULL);
    tm_ptr = std::localtime(&now);

    std::strftime(time_buffer, TIME_SIZE, "%d %B %Y %I:%M:%S %p", tm_ptr);

    std::cout << time_buffer << "\n";

    return;
#undef TIME_SIZE
}