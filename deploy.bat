dotnet publish F:\scratch\spark\examples\Microsoft.Spark.CSharp.Examples\Microsoft.Spark.CSharp.Examples.csproj -f netcoreapp2.1 -r win-x64
dotnet publish F:\scratch\spark\src\csharp\Microsoft.Spark.Worker\Microsoft.Spark.Worker.csproj -f netcoreapp2.1 -r win-x64
pushd F:\scratch\spark\artifacts\bin\Microsoft.Spark.CSharp.Examples\Debug\netcoreapp2.1\win-x64\publish\
copy /Y Microsoft.Spark.CSharp.Examples* F:\scratch\spark\artifacts\bin\Microsoft.Spark.Worker\Debug\netcoreapp2.1\win-x64\publish\
copy /Y System.* F:\scratch\spark\artifacts\bin\Microsoft.Spark.Worker\Debug\netcoreapp2.1\win-x64\publish\
copy /Y Microsoft.Bcl.AsyncInterfaces* F:\scratch\spark\artifacts\bin\Microsoft.Spark.Worker\Debug\netcoreapp2.1\win-x64\publish\
popd
dotnet restore F:\scratch\spark\examples\Microsoft.Spark.CSharp.Examples\Microsoft.Spark.CSharp.Examples.csproj
dotnet restore F:\scratch\spark\src\csharp\Microsoft.Spark.Worker\Microsoft.Spark.Worker.csproj
taskkill /f /im dotnet.exe
