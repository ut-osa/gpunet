#!/bin/bash

# Details on -gencode options
#  - arch: front-end target (PTX version, or compute capabitiliy version in CUDA guide appendix G)
#  - code: back-end target (PTX or cubin version) Only this is retained in the result.
#    - PTX (compute_XX) in either of options are often for FUTURE COMPATIBILITY

#  - compute_XX: PTX version.
#  - sm_XX: cubin version.

#  -arch=sm_XX: short hand of -gencode=arch=compute_XX,code=sm_XX -gencode=arch=compute_XX,code=compute_XX

NVCC_VERSION=$(nvcc --version | egrep -o 'release [0-9.]+' | awk '{ print $2; }')

# higher preference GPUs come first
TARGET_GPUS="K20c K20Xm C2075"

function usage() {
    echo "Usage: $1 [options]"
    echo 
    echo "  generates compiler arch/code option for the GPU with the highest priority."
    echo 
    echo "  options:"
    echo "    --all: generates all versions of code for 2.x/3.0/3.5"
    echo "    --help: shows this usage"
    echo 
    echo "Supported GPUs: ${TARGET_GPUS}"
    echo "Available GPUs: $(nvidia-smi --list-gpus | cut -d' ' -f 4 | sed 's/$/,/g' | xargs echo -n)"
    echo "  * Available GPUs may not give the model numbers for old GPUs."
}

if [[ -z ${NVCC_VERSION} ]]; then
    echo "NVCC or its version is not available."
    exit 1
fi

if [[ ${NVCC_VERSION:0:1} != "5" ]]; then
    echo "Only NVCC version 5.x is supported"
    exit 1
fi

if [[ "$1" == "--all" ]]; then
    echo "-gencode=arch=compute_20,code=sm_20 -gencode=arch=compute_30,code=sm_30 -gencode=arch=compute_35,code=sm_35 -gencode=arch=compute_35,code=compute_35"
    exit 0
fi

if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
    usage
    exit 0
fi


# nvidia-smi may be slow, so cache it
#
# place cache file on tmpfs for its ephemerality
CACHE_FILE=/tmp/cache_nvcc_option_gen.sh
if [[ -e ${CACHE_FILE} ]]; then
    NVIDIA_GPUS=$(cat ${CACHE_FILE})
else
    NVIDIA_GPUS=$(nvidia-smi --list-gpus | cut -d' ' -f 3-4)
    echo ${NVIDIA_GPUS} > ${CACHE_FILE}
fi

GPU=
for target_gpu in ${TARGET_GPUS}; do
    for gpu in ${NVIDIA_GPUS}; do
        if [[ $gpu =~ .*${target_gpu}.* ]]; then
            GPU=$target_gpu
            break
        fi
    done
    if [[ ! -z $GPU ]]; then
        break
    fi
done

if [[ -z $GPU ]]; then
    echo "No appropriate GPU available: " ${NVIDIA_GPUS}
    exit 1
fi

case "$GPU" in
    K20c)
        echo "-gencode arch=compute_35,code=sm_35"
        ;;
    K20Xm)
        echo "-gencode arch=compute_35,code=sm_35"
        ;;
    C2075)
        echo "-gencode arch=compute_20,code=sm_20"
        ;;
    *)
        echo "ERROR: you need to add a handler for the GPU " $GPU
        exit 1
        ;;
esac
