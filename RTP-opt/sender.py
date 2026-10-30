import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

KICH_THUOC = 1456
TIMEOUT = 0.5


def tao_goi(loai, stt, du_lieu=b""):
    dau = PacketHeader(type=loai, seq_num=stt, length=len(du_lieu), checksum=0)
    goi = dau / du_lieu
    goi[PacketHeader].checksum = compute_checksum(goi)
    return bytes(goi)


def doc_ack(du_lieu):
    if len(du_lieu) != 16:
        return None

    try:
        dau = PacketHeader(du_lieu)
        if dau.type != 3 or dau.length != 0:
            return None

        ma = dau.checksum
        dau.checksum = 0

        if compute_checksum(dau) != ma:
            return None

        return dau.seq_num
    except Exception:
        return None


def sender(receiver_ip, receiver_port, window_size):
    if window_size <= 0:
        raise ValueError("window_size phai lon hon 0")

    dia_chi = (receiver_ip, receiver_port)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        goi_start = tao_goi(0, 0)
        s.settimeout(TIMEOUT)
        bat_dau = time.monotonic()
        nhan_start = False

        while time.monotonic() - bat_dau < 10:
            s.sendto(goi_start, dia_chi)

            try:
                du_lieu, _ = s.recvfrom(2048)
            except socket.timeout:
                continue

            if doc_ack(du_lieu) == 1:
                nhan_start = True
                break

        if not nhan_start:
            print("Khong nhan duoc ACK START", file=sys.stderr)
            return

        du_lieu = sys.stdin.buffer.read()
        cac_goi = [
            du_lieu[i:i + KICH_THUOC]
            for i in range(0, len(du_lieu), KICH_THUOC)
        ]

        tong = len(cac_goi)
        goi_data = {
            i + 1: tao_goi(2, i + 1, nd)
            for i, nd in enumerate(cac_goi)
        }

        dau = 1
        tiep = 1
        da_ack = set()
        dang_cho = {}
        moc = None

        while dau <= tong:
            while tiep <= tong and tiep < dau + window_size:
                goi = goi_data[tiep]
                s.sendto(goi, dia_chi)
                dang_cho[tiep] = goi
                tiep += 1

                if moc is None:
                    moc = time.monotonic()

            if moc is None:
                moc = time.monotonic()

            con_lai = TIMEOUT - (time.monotonic() - moc)

            if con_lai <= 0:
                for stt, goi in dang_cho.items():
                    if stt not in da_ack:
                        s.sendto(goi, dia_chi)

                moc = time.monotonic()
                continue

            s.settimeout(con_lai)

            try:
                du_lieu_ack, _ = s.recvfrom(2048)
            except socket.timeout:
                for stt, goi in dang_cho.items():
                    if stt not in da_ack:
                        s.sendto(goi, dia_chi)

                moc = time.monotonic()
                continue

            ack = doc_ack(du_lieu_ack)

            if ack is None or ack not in dang_cho:
                continue

            da_ack.add(ack)
            co_tien = False

            while dau in da_ack:
                da_ack.remove(dau)
                dang_cho.pop(dau, None)
                dau += 1
                co_tien = True

            if co_tien:
                moc = time.monotonic() if dang_cho else None

        goi_end = tao_goi(1, tong + 1)
        s.sendto(goi_end, dia_chi)
        het_han = time.monotonic() + TIMEOUT
        s.settimeout(TIMEOUT)

        while time.monotonic() < het_han:
            s.settimeout(max(0.01, het_han - time.monotonic()))

            try:
                du_lieu_ack, _ = s.recvfrom(2048)
            except socket.timeout:
                break

            if doc_ack(du_lieu_ack) == tong + 2:
                break

    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
