# Build Instructions

1. Download or clone the repository that contains:
   - `Node.java` (your main CRN node implementation)
   - This `README.md`

2. Compile:
   - Open a terminal inside the project folder.
   - Compile all `.java` files:

```bash
javac *.java
```

   - This will generate the `.class` files for each of your Java classes.

3. Run:
   - To test locally with the `LocalTest` class (if provided), do:

```bash
java LocalTest
```
     
   - If you have a separate test program (like `AzureLabTest.java`), run it similarly but on a VM:

```bash
java AzureLabTest
```


## Working Functionality

- **Message Parsing and Handling:** All CRN message types (W, R, E, C, G, N, V, and I) are recognised.

- **Condition A/B Logic:** We correctly check whether we already store a key (A) and whether we are among the 3 closest nodes to the key (B) for read, write, exists, and CAS requests.

- **Storing Up to 3 Addresses per Distance:** The code enforces a maximum of three address entries for each distance.

- **Iterative Nearest-Node Lookups:** For read/write/exists/CAS, we do a BFS to discover actual closest nodes.

- **Resending Requests:** Each request is sent up to 3 times if no response is received within a few seconds, matching the CRN “MUST resend” requirement.

- **Reading/Writing:** We can read data that we store locally or data from remote nodes, assuming we discover the correct nearest node(s).

- **CAS:** The compare-and-swap implementation checks existing values and replaces them atomically if they match.

If run against the provided `LocalTest` and `AzureLabTest` (or any typical CRN test harness), the `Node` implementation should successfully store and retrieve data, handle address keys, and pass the major f[...]
