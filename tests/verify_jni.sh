#!/bin/bash
# Pre-flight JNI Linkage Verifier

LIB_SO="native/target/release/libnative_pubsub_connector.so"

echo "=== Auditing Native Symbols in $LIB_SO ==="
nm -D "$LIB_SO" | grep Java_com_google_cloud_spark_pubsub | cut -d' ' -f3 | sort > symbols.txt

echo -e "\n=== Auditing Scala Signatures (Native Methods) ==="
# Find all classes in the target directory
ROOT="spark/spark35/target/scala-2.12/classes"
find "$ROOT"/com/google/cloud/spark/pubsub -name "*.class" | while read -r classfile; do
    # Extract package and class name
    rel_path=${classfile#$ROOT/}
    classname_dotted=$(echo "${rel_path%.class}" | tr '/' '.')
    
    # Check for native methods using javap
    native_methods=$(javap -p -s "$classfile" | grep -A 1 "native")
    
    if [ -n "$native_methods" ]; then
        echo "Class: $classname_dotted"
        echo "$native_methods" | while read -r line; do
            if [[ $line == *"native"* ]]; then
                # Extract method name
                method=$(echo "$line" | awk '{print $NF}' | cut -d'(' -f1)
                # JNI Mangle: Java_package_path_ClassName_methodName
                # Note: This is a simplified mangling (doesn't handle underscores in names or overloads perfectly, but enough for our classes)
                mangled_class=$(echo "$classname_dotted" | tr '.' '_')
                expected="Java_${mangled_class}_$method"
                
                printf "  Checking: %-60s -> " "$method"
                if grep -x "$expected" symbols.txt > /dev/null; then
                    echo "[OK] ($expected)"
                else
                    echo -e "\033[0;31m[MISSING]\033[0m (Expected: $expected)"
                fi
            fi
        done
    fi
done

rm symbols.txt
