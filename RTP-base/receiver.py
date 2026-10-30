
import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

KICH_THUOC_DAU = 16


def tao_ack(stt):
    dau = PacketHeader(type=3, seq_num=stt, length=0, checksum=0)
    dau.checksum = compute_checksum(dau)
    return bytes(dau)


def doc_goi(du_lieu):
    if len(du_lieu) < KICH_THUOC_DAU:
        return None

    try:
        dau = PacketHeader(du_lieu[:KICH_THUOC_DAU])
        noi_dung = du_lieu[KICH_THUOC_DAU:]

        if dau.length != len(noi_dung):
            return None

        ma = dau.checksum
        dau.checksum = 0

        goi = dau / noi_dung

        if ma != compute_checksum(goi):
            return None

        return dau, noi_dung

    except Exception:
        return None


def receiver(receiver_port, window_size):
    if window_size <= 0:
        raise ValueError("window_size phai lon hon 0")

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("", receiver_port))

    cho = {}
    tiep_theo = 1
    bat_dau = False

    try:
        while True:
            du_lieu, dia_chi = s.recvfrom(65535)
            ket_qua = doc_goi(du_lieu)

            if ket_qua is None:
                continue

            dau, noi_dung = ket_qua
            loai = dau.type
            stt = dau.seq_num

            if loai == 0:
                bat_dau = True
                tiep_theo = 1
                cho.clear()
                s.sendto(tao_ack(1), dia_chi)
                continue

            if not bat_dau:
                continue

            if loai == 2:
                if stt < tiep_theo:
                    s.sendto(tao_ack(tiep_theo), dia_chi)
                    continue

                if stt >= tiep_theo + window_size:
                    s.sendto(tao_ack(tiep_theo), dia_chi)
                    continue

                if stt not in cho:
                    cho[stt] = noi_dung

                while tiep_theo in cho:
                    sys.stdout.buffer.write(cho.pop(tiep_theo))
                    sys.stdout.buffer.flush()
                    tiep_theo += 1

                s.sendto(tao_ack(tiep_theo), dia_chi)

            elif loai == 1:
                if stt == tiep_theo:
                    s.sendto(tao_ack(stt + 1), dia_chi)
                    break

                s.sendto(tao_ack(tiep_theo), dia_chi)

    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()

    receiver(args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
