#ifndef _CONN_UTILS_
#define _CONN_UTILS_
#include <iostream>
namespace hurricane {
extern unsigned long baseport;

  static const std::string protocol = "tcp";
  static const std::string separator = ":";

    // Number of endpoints
  static unsigned long endpoint_count(unsigned long machines) {
    std::cout << machines << " " << machines << " " << "1 + 1" << std::endl;
    return machines + //server data in
           machines + //client data in
           1 + // server control
           1;  // barrier
  }

    // Utility functions to generate endpoints for connections
  static std::string conn_server_data_endpoint(unsigned long mc_target,
                                        unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://server";
    } else {
      addr << protocol << "://";
      addr << pt_hurricane.get<std::string>(name.str().c_str());
      unsigned long machines = pt_hurricane.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static std::string conn_server_data_bindpoint(unsigned long mc_target,
                                         unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://server";
    } else {
      addr << protocol << "://";
      addr << pt_hurricane.get<std::string>(name.str().c_str());
      unsigned long machines = pt_hurricane.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           mc_source;
      std::cout << baseport << " + '" << endpoint_count(machines) << "'*" << mc_target << " + " << mc_source << " = " << port << std::endl;
      addr << separator << port;
    }
    return addr.str();
  }

  static std::string conn_client_data_endpoint(unsigned long mc_target,
                                        unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://client";
    } else {
      addr << protocol << "://";
      addr << pt_hurricane.get<std::string>(name.str().c_str());
      unsigned long machines = pt_hurricane.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           machines + mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static std::string conn_client_data_bindpoint(unsigned long mc_target,
                                         unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://client";
    } else {
      addr << protocol << "://";
      addr << pt_hurricane.get<std::string>(name.str().c_str());
      unsigned long machines = pt_hurricane.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           machines + mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static std::string conn_server_control_endpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    addr << protocol << "://";
    addr << pt_hurricane.get<std::string>(name.str().c_str());
    unsigned long machines = pt_hurricane.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines;
    addr << separator << port;
    return addr.str();
  }

  static std::string conn_server_control_bindpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    addr << protocol << "://";
    addr << pt_hurricane.get<std::string>(name.str().c_str());
    unsigned long machines = pt_hurricane.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines;
    addr << separator << port;
    return addr.str();
  }

  static std::string conn_client_barrier_endpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    addr << protocol << "://";
    addr << pt_hurricane.get<std::string>(name.str().c_str());
    unsigned long machines = pt_hurricane.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines + 1;
    addr << separator << port;
    return addr.str();
  }

  static std::string conn_client_barrier_bindpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    addr << protocol << "://";
    addr << pt_hurricane.get<std::string>(name.str().c_str());
    unsigned long machines = pt_hurricane.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines + 1;
    addr << separator << port;
    return addr.str();
  }
}
#endif