package in.dream_lab.echo.utils;

import java.util.List;
import java.util.Map;

/**
 * Created by pushkar on 5/22/17.
 */
public interface NifiClient {

    String get_url();
    String get_process_group_id();
    String create_processor(String _class, String name);
    boolean set_processor_properties_and_relationships(String processorID,
                                                       String propertiesJson,
                                                       String relationshipsJson,
                                                       String config);
    // TODO make it work with multiple relationships
    boolean create_connection(String from_id, String to_id, String relationship_name);
    String create_new_input_port(String name);
    String create_new_output_port(String name);
    boolean delete_input_port(String id);
    boolean delete_output_port(String id);
    boolean delete_processor(String processor_id);
    boolean enable_input_port(String port_id);
    boolean enable_output_port(String port_id);
    boolean disable_input_port(String port_id);
    boolean disable_output_port(String port_id);
    String create_remote_process_group(String target_uri);
    boolean connect_remote_input_port(String rpg_id, String port_id, String proc_id, String relationships);
    boolean connect_remote_output_port(String rpg_id, String port_id, String proc_id, String relationships);
    boolean enable_rpg_transmission(String rpg_id);
    boolean disable_rpg_transmission(String rpg_id);
    boolean start_processor(String processor_id);
    boolean stop_processor(String processor_id);
    boolean delete_connection(String connection_id);
    boolean delete_remote_process_group(String rpg_id);
    List<Connection> get_connections();
}
