#include <iostream>
#include <memory>
#include <bits/stdc++.h>
// #include <netcdf>

// We are writing 2D data, a 6 x 12 grid
constexpr int nx = 6;
constexpr int ny = 12;

// Return this in event of a problem
constexpr int nc_err = 2;

using std::cout;
using std::endl;
using std::shared_ptr;

void sort_(double precip_set[])
{
    int size_array = sizeof(precip_set) / sizeof((int)precip_set[0]);

    std::sort(precip_set, precip_set + size_array);
}

double maximum(double precip_set[])
{
    int size_array = *(&precip_set + 1) - precip_set - 1;

    return *std::max_element(precip_set, precip_set + size_array);
}

double minumum(double precip_set[])
{
    int size_array = *(&precip_set + 1) - precip_set - 1;

    return *std::min_element(precip_set, precip_set + size_array);
}

double median(double precip_set[])
{
    int size_array = *(&precip_set + 1) - precip_set - 1;

    // even number of elements
    if (size_array % 2 != 0)
    {
        return precip_set[size_array / 2];
    }

    return (precip_set[(size_array - 1) / 2] + precip_set[size_array / 2]) / 2.0;
}

double lower_quartile(double precip_set[])
{
}

int main()
{
    double precip[5] = {3, 2, 5, 6, 7};

    sort_(precip);

    for (auto i = 0; i < 5; i++)
    {
        cout << precip[i] << endl;
    }

    // // Now read the data back in
    // try
    // {
    //     // This is the array we will read into
    //     int dataIn[nx][ny];

    //     // Open the file for read access
    //     netCDF::NcFile dataFile("air.sig995.2012.nc", netCDF::NcFile::read);

    //     // Retrieve the variable named "data"
    //     auto data = dataFile.getVar("data");
    //     if (data.isNull())
    //         return nc_err;
    //     data.getVar(dataIn);

    //     // Check the values.
    //     for (int i = 0; i < nx; i++)
    //     {
    //         for (int j = 0; j < ny; j++)
    //         {
    //             if (dataIn[i][j] != i * ny + j)
    //             {
    //                 return nc_err;
    //             }
    //         }
    //     }
    // }
    // catch (netCDF::exceptions::NcException &e)
    // {
    //     std::cout << e.what() << std::endl;
    //     return nc_err;
    // }

    return 0;
}