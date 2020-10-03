package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {

	private Connection conn;

	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		PreparedStatement st = null;
		try {

			st = conn.prepareStatement("INSERT INTO department " + "(Name) " + "VALUES " + "(?)",
					Statement.RETURN_GENERATED_KEYS);

			st.setString(1, obj.getName());

			int rowsAffected = st.executeUpdate();
			
			if (rowsAffected > 0) {

				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);

					DB.closeResultSet(rs);
				}

				else {
					throw new DbException("Erro inesperado: Nenhuma linha foi inserida");

				}
			}
		} catch (SQLException e1) {
			throw new DbException(e1.getMessage());
		} finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public void update(Department obj) {

		PreparedStatement st = null;
		try {

			st = conn.prepareStatement("UPDATE department " + "SET Name = ?" + "WHERE ID = ?");

			st.setString(1, obj.getName());
			st.setInt(2, obj.getId());

			st.executeUpdate();

		} catch (SQLException e1) {
			throw new DbException(e1.getMessage());
		} finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public void deleteById(String name) {

		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE FROM department WHERE Name = ? ");

			st.setString(1, name);
			int rows = st.executeUpdate();

			if (rows == 0) {
				throw new DbException("O nome informado não existe!");
			}
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Department findById(Integer id) {

		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement("SELECT * " + " FROM department " + " WHERE department.Id = ?");
			st.setInt(1, id);
			rs = st.executeQuery();
			if (rs.next()) {

				Department obj = instanciateDepartment(rs);
				return obj;

			}
			return null;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Department instanciateDepartment(ResultSet rs) throws SQLException {

		Department dep = new Department();
		dep.setId(rs.getInt("ID"));
		dep.setName(rs.getString("Name"));

		return dep;
	}

	@Override
	public List<Department> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement("SELECT Id, Name " + " FROM  department");

			rs = st.executeQuery();
			
			List<Department> list = new ArrayList<>();
			while (rs.next()) {
				Department obj = new Department();
				obj.setId(rs.getInt("ID"));
				obj.setName(rs.getString("Name"));
				list.add(obj);
			}
			return list;
		}

		catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}

	}

}
