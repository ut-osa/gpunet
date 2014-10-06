#!/bin/bash

SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

make -C ${SRC_DIR}

if [[ $(lsb_release --release --short) == "6.5" ]]; then
	NV_PEER_MEM=1
else
	NV_PEER_MEM=0
fi

if ibstatus mlx4_0:1 | grep -i active &> /dev/null; then
	echo "CHECK: Low-level Infiniband (ib0) is working well."
else
	echo "Low-level Infiniband (ib0) is not active."

	if ibstatus mlx4_0:1 | grep -i linkup &> /dev/null ; then
		echo "Hardware is working, so it must be some driver issue. Wait 10 seconds.."
		sleep 10

		if ibstatus mlx4_0:1 | grep -i active &> /dev/null; then
			echo "Now things seem fine."
		else
			echo "Still things do not look fine."
			echo
			echo "Trying running opensmd, and sleep 10 seconds"
			sudo service opensmd restart
			sleep 10
			if ibstatus mlx4_0:1 | grep -i active &> /dev/null ; then
				echo "Voila. Now it works."
			else
				echo "No it doesn't help. Crap."
				exit 1
			fi
		fi
	else
		echo "ERROR: IB Hardware is not working well."
		echo "Please check your hardware setting."
		echo "This might be cable plugged into another port than ib0."
		exit 1
	fi
	exit 1
fi

HOSTNAME=$(uname --nodename)
HOSTNAME=${HOSTNAME%.csres.utexas.edu}

if [[ ${HOSTNAME} =~ tesla.. ]]; then
	IB0_IPADDR=$(ip addr show dev ib0 | grep "inet " | awk '{ print $2 }')
	IB0_IPADDR=${IB0_IPADDR%/24}

	EXPECTED_IPADDR=192.168.10.$(awk "BEGIN{print 10 + ${HOSTNAME#tesla}; }")

	echo "We want ${EXPECTED_IPADDR} to be this machine IP address"
	
	if [[ -z ${IB0_IPADDR} ]]; then
		echo "IB0 needs address"
		sudo ifconfig ib0 ${EXPECTED_IPADDR}
	fi

	if [[ ${IB0_IPADDR} != ${EXPECTED_IPADDR} ]]; then
		echo "ERROR: IP address ${IB0_IPADDR} different from the expected one ${EXPECTED_IPADDR}"
		exit 1
	else
		echo "CHECK: IP address looks fine"
	fi
fi

USERMAP_DEV=/dev/gpu_usermap
if [[ ! -e ${USERMAP_DEV} ]]; then
	echo "gpu_usermap needs installation."
	sudo modprobe gpu_usermap
else
	echo "CHECK: gpu_usermap is installed."
fi

if [[ ${NV_PEER_MEM} == "1" ]]; then
	if lsmod | grep nv_peer_mem &> /dev/null; then
		echo "CHECK: nv_peer_mem is installed."
	else
		echo "nv_peer_mem module needs to be installed."
		if ! sudo modprobe nv_peer_mem; then
			echo "nv_peer_mem installation failed"
			exit 1
		fi
		sleep 1

		if lsmod | grep nv_peer_mem &> /dev/null; then
			echo "CHECK: nv_peer_mem is now installed."
		else
			echo "ERROR: nv_peer_mem is still not installed."
			exit 1
		fi
	fi
fi

echo "SUCCEEDED!"